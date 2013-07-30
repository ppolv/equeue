-module(equeue).

-behaviour(gen_server).

%%API
-export([start_link/1,push/2, active_once/1, recv/1, stop_recv/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).


-record(state, {
        size :: integer(),   %%If the queue has less element than this this value, producer don't block on push
        queue :: queue(),
        blocked_senders :: queue(),   
        current_size :: integer(),
        subscribed_workers :: [term()]   %%  [active_once | gen_server:call]  subscribers
        }).

push(Queue, Item) ->
    gen_server:call(Queue, {push, Item}, infinity).

active_once(Queue) ->
    gen_server:call(Queue, {request_work, {active_once, self()}}).

recv(Queue) ->
    gen_server:call(Queue, {request_work, blocking}, infinity).

stop_recv(Queue) ->
    gen_server:call(Queue, {stop_recv, self()}).


start_link(Size) ->
    gen_server:start_link(?MODULE, [Size], []).


init([Size]) ->
    {ok, #state{size = Size, queue = queue:new(), current_size = 0, subscribed_workers = [], blocked_senders=queue:new()}}.

%% TODO monitor the subscribers
handle_call({request_work, RequestType}, From, State = #state{current_size = 0, subscribed_workers = S}) ->
    case RequestType of
        blocking ->
            {noreply, State#state{subscribed_workers = [{blocking, From} | S]}};
        {active_once, Pid} ->
            {reply, ok, State#state{subscribed_workers = [{active_once, Pid} | S]}}
    end;
handle_call({request_work, RequestType}, _From, State = #state{queue =Q, current_size = QS, blocked_senders = Blocked}) ->
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
        {active_once, Pid} ->
            Pid ! {job, Job},
            {reply, ok, State#state{queue = NewQueue, current_size = NewQS, blocked_senders = NB}}
    end;
handle_call({push, Job}, _From, State = #state{queue = Q, current_size = 0, subscribed_workers = S}) ->
    case S of
        [{active_once, W}|S2] ->
            W ! {job, Job},
            {reply, ok, State#state{subscribed_workers = S2}};
        [{blocking, W}|S2] ->
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
        {reply, ok, State#state{subscribed_workers = lists:delete({active_once,Pid}, S)}}.



handle_cast(_Cast, State) ->
    {noreply, State}.
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

 

