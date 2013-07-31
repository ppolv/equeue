-module(equeue).

-behaviour(gen_server).

%%API
-export([start_link/1, start_link/2, push/2, active_once/1, recv/1, stop_recv/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
        size :: integer(),   %%If the queue has less element than this this value, producer don't block on push
        queue :: queue(),
        blocked_senders :: queue(),   
        current_size :: integer(),
        subscribed_workers :: [term()]   %%  [{active_once, blocking, pid()|from(), monitor_ref()}]  subscribers
        }).


push(Queue, Item) ->
    gen_server:call(Queue, {push, Item}, infinity).

active_once(Queue) ->
    gen_server:call(Queue, {request_work, {active_once, self()}}).

recv(Queue) ->
    gen_server:call(Queue, {request_work, {blocking, self()}}, infinity).

stop_recv(Queue) ->
    gen_server:call(Queue, {stop_recv, self()}).


start_link(Size) ->
    gen_server:start_link(?MODULE, [Size], []).
start_link(Name, Size) ->
    gen_server:start_link({local, Name}, ?MODULE, [Size], []).


init([Size]) ->
    {ok, #state{size = Size, queue = queue:new(), current_size = 0, subscribed_workers = [], blocked_senders=queue:new()}}.

%% TODO monitor the subscribers
handle_call({request_work, {RequestType, Pid}}, From, State = #state{current_size = 0, subscribed_workers = S}) ->
    MRef = monitor(process, Pid),
    case RequestType of
        blocking ->
            {noreply, State#state{subscribed_workers = [{blocking, From, MRef} | S]}};
        active_once ->
            {reply, ok, State#state{subscribed_workers = [{active_once, Pid, MRef} | S]}}
    end;
handle_call({request_work, {RequestType, Pid}}, _From, State = #state{queue =Q, current_size = QS, blocked_senders = Blocked}) ->
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
    case RequestType of
        blocking ->
            {reply, {ok, Job}, State#state{queue = NewQueue, current_size = NewQS, blocked_senders = NB}};
        active_once ->
            Pid ! {job, Job},
            {reply, ok, State#state{queue = NewQueue, current_size = NewQS, blocked_senders = NB}}
    end;
handle_call({push, Job}, _From, State = #state{queue = Q, current_size = 0, subscribed_workers = S}) ->
    case S of
        [{active_once, W, MRef}|S2] ->
            demonitor(MRef, [flush]),
            W ! {job, Job},
            {reply, ok, State#state{subscribed_workers = S2}};
        [{blocking, W, MRef}|S2] ->
            demonitor(MRef, [flush]),
            gen_server:reply(W,{ok, Job}),
            {reply, ok, State#state{subscribed_workers = S2}};
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
handle_info({'DOWN', MonitorRef, _, _, _}, State = #state{subscribed_workers = S}) ->
    {noreply, State#state{subscribed_workers = lists:keydelete(MonitorRef, 3, S)}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

 

