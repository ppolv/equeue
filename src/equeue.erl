-module(equeue).

-behaviour(gen_server).

%%API
-export([start_link/1, start_link/2, push/2, active_once/2, recv/2, stop_recv/1, register_worker/1, get_state/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
        size :: integer(),   %%If the queue has less element than this this value, producer don't block on push
        queue :: queue(),
        blocked_senders :: queue(),   
        current_size :: integer(),
        subscribed_workers :: [term()],   %%  [{active_once, blocking, pid()|from(), monitor_ref()}]  subscribers
        workers :: [], %% workers that have registered with us, they are monitored. If there is no worked registered, pushes are rejected.
        ongoing_work :: [] %% start a timer for workers that have requested job. If they don't ask for job again in X time, we kill them as
                           %% a countermeasure for bugs where they might become stuck.
        }).

%% Note we use simple lists []  instead of dicts/tree as the expected number of workers per queue is low, < 20


push(Queue, Item) ->
    gen_server:call(Queue, {push, Item}, infinity).

active_once(Queue, KillIfNotDoneIn) ->
    gen_server:call(Queue, {request_work, {active_once, self(), KillIfNotDoneIn}}).

recv(Queue, KillIfNotDoneIn) ->
    gen_server:call(Queue, {request_work, {blocking, self(), KillIfNotDoneIn}}, infinity).

stop_recv(Queue) ->
    gen_server:call(Queue, {stop_recv, self()}).

register_worker(Queue) ->
    gen_server:call(Queue, {register_worker, self()}).

start_link(Size) ->
    gen_server:start_link(?MODULE, [Size], []).
start_link(Name, Size) ->
    gen_server:start_link({local, Name}, ?MODULE, [Size], []).

%% {ok,[{key(),val()}]}.
%% key() = current_size|max_size|blocked_senders|registered_listeners|blocked_listeners
%% val() = int()
get_state(Queue) ->
    gen_server:call(Queue, get_state).

init([Size]) ->
    {ok, #state{size = Size, 
            queue = queue:new(), 
            current_size = 0, 
            subscribed_workers = [], 
            blocked_senders=queue:new(), 
            workers=[], 
            ongoing_work = []}}.
handle_call(get_state, _From, State) ->
   #state{size = Size, 
          current_size = CurrentSize, 
          subscribed_workers = Subscribed, 
          blocked_senders=Blocked, 
          workers=Registered
          } = State,
    Info = [ {current_size, CurrentSize}, 
        {max_size, Size}, 
        {blocked_senders, length(queue:to_list(Blocked))}, 
        {registered_listeners, length(Registered)}, 
        {blocked_listeners, length(Subscribed)}],
    {reply, {ok, Info}, State};

handle_call({register_worker,  Pid}, _From, State = #state{workers = Registered}) ->
    MRef = erlang:monitor(process, Pid),
    {reply, ok, State#state{workers = lists:keystore(Pid, 1, Registered, {Pid,MRef})}};
handle_call({request_work, {_,Pid, _} = Request}, From, State = #state{workers=Registered}) ->
    case lists:keymember(Pid, 1, Registered) of
        true ->
            do_request_work(Request,From, State);
        false ->
            {reply, {error, no_registered_as_worker}, State}
    end;
handle_call({push, _Job}, _From, State = #state{workers = []}) ->
        {reply, {error, {no_worker_on_queue, self()}}, State};
handle_call({push, Job}, _FromPush, State = #state{queue = Q, current_size = 0, subscribed_workers = S}) ->
    case S of
        [{active_once, Pid, _, KillIfNotDoneIn}|S2] ->
            NewState = monitor_job(Pid, KillIfNotDoneIn, Job, State),
            Pid ! {job, Job},
            {reply, ok, NewState#state{subscribed_workers = S2}};
        [{blocking, Pid, From, KillIfNotDoneIn}|S2] ->
            NewState = monitor_job(Pid, KillIfNotDoneIn, Job, State),
            gen_server:reply(From,{ok, Job}),
            {reply, ok, NewState#state{subscribed_workers = S2}};
        [] ->
            {reply, ok, State#state{queue = queue:in(Job, Q), current_size =1}}
    end;
handle_call({push, Job}, From, State = #state{queue = Q, current_size = QS, size = Size}) ->
    NewQueue = queue:in(Job, Q),
    NewQS = QS +1,
    if 
        NewQS > Size ->
            %% must block sender
            {noreply, State#state{queue = NewQueue, 
                                  current_size = NewQS, 
                                  blocked_senders = queue:in(From,State#state.blocked_senders)}};
        true ->
            {reply, ok, State#state{queue = NewQueue, current_size = NewQS}}
    end;

handle_call({stop_recv, Pid}, _From, State = #state{subscribed_workers = S}) ->
        {reply, ok, State#state{subscribed_workers = lists:keydelete(Pid, 2, S)}}.



handle_cast(_Cast, State) ->
    {noreply, State}.
handle_info({worker_stuck, Pid}, State = #state{ongoing_work = S}) ->
    case lists:keytake(Pid, 1, S) of
        false ->
            {noreply, State};  %%not found, should be a late message
        {value, {Pid, _TRef, Job}, NewList} ->
            catch kill_worker(self(), Pid, Job),
            {noreply, State#state{ongoing_work = NewList}}
    end;
handle_info({'DOWN', _MonitorRef, _, Pid, _}, State = #state{subscribed_workers = S}) ->
    {noreply, State#state{
            subscribed_workers = lists:keydelete(Pid, 2, S), 
            workers=lists:keydelete(Pid,1, State#state.workers),
            ongoing_work = lists:keydelete(Pid, 1, State#state.ongoing_work)}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Don't print the job as it could be large, containing many tokens :/.  The info would be in the backtrace anyway.
kill_worker(Queue, Pid, _Job) ->
    io:format("killing ~p from queue ~p: ~p", [Pid, Queue, _Job]),
    BackTrace = process_info(Pid, backtrace),
    exit(Pid, kill),
    lager:error("Queue ~p killing worker process ~p with backtrace: ~p", [Queue, Pid, BackTrace]),
    ok.

do_request_work({_, Pid, _}=Request, From, State = #state{ongoing_work = L}) ->
    NewState = case lists:keytake(Pid, 1, L) of
                    false ->
                        State;  %% we don't have it.. should not happen
                    {value, {Pid, TRef, _Job}, NewList} ->  
                        %%this worker is alive and asking for more jobs, cancel the timer
                        erlang:cancel_timer(TRef),
                        State#state{ongoing_work = NewList} 
                end,
    do_request_work2(Request, From, NewState).

do_request_work2({RequestType, Pid, KillIfNotDoneIn}, From, State = #state{current_size = 0, subscribed_workers = S}) ->
    case RequestType of
        blocking ->
            {noreply, State#state{subscribed_workers = [{blocking, Pid, From, KillIfNotDoneIn} | S]}};
        active_once ->
            {reply, ok, State#state{subscribed_workers = [{active_once, Pid, From, KillIfNotDoneIn} | S]}}
    end;
do_request_work2({RequestType, Pid, KillIfNotDoneIn}, _From, State = #state{queue =Q, current_size = QS, blocked_senders = Blocked}) ->
    NewQS = QS -1,
    {{value, Job}, NewQueue} =  queue:out(Q),
    NB = if 
        QS > State#state.size ->
            {{value, B}, NewBlocked} = queue:out(Blocked),
            gen_server:reply(B, ok),
            NewBlocked;
        true ->
            Blocked
    end,
    NewState = monitor_job(Pid, KillIfNotDoneIn, Job, State),
    case RequestType of
        blocking ->
            {reply, {ok, Job}, NewState#state{queue = NewQueue, current_size = NewQS, blocked_senders = NB}};
        active_once ->
            Pid ! {job, Job},
            {reply, ok, NewState#state{queue = NewQueue, current_size = NewQS, blocked_senders = NB}}
    end.

monitor_job(Pid, KillIfNotDoneIn, Job, State) ->
    TRef = erlang:send_after(KillIfNotDoneIn, self(), {worker_stuck, Pid} ),
    State#state{ongoing_work = [{Pid, TRef, Job} | State#state.ongoing_work]}.




