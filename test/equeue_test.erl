-module(equeue_test).

-include_lib("eunit/include/eunit.hrl").

-define(PARALELL_SENDERS,5).
-define(JOB_TIMEOUT, 100). %0.1 second

blocking_worker(Queue, Parent) ->
    {ok, J} = equeue:recv(Queue, ?JOB_TIMEOUT),
    Parent ! {out, J},
    blocking_worker(Queue, Parent).

blocking_test() ->
    {ok,Q} = equeue:start_link(10),  %%a queue of size 10
    Parent = self(),
    lists:foreach(fun(_) ->
                spawn(fun() ->
                            ok = equeue:register_worker(Q),
                            blocking_worker(Q, Parent) end)
        end, lists:seq(1,2)), %%2 receivers
    _Senders = lists:foreach(fun(I) -> 
                spawn(fun() ->
                            lists:foreach(fun(J) -> 
                                        Item = {I,J},
                                        equeue:push(Q, Item),   %%10 pushes each sender
                                        Parent ! {in, Item}
                                end, lists:seq(1,10))
                    end)
        end,  lists:seq(1,?PARALELL_SENDERS)), %5 senders
    All = receive_all(100), 
    ?assertEqual(length(All), 100),
    check_results(All).


worker(Queue, Parent) ->
    equeue:active_once(Queue, ?JOB_TIMEOUT),
    receive
        X ->
            Parent ! {out,X}
    end,
    timer:sleep(random:uniform(20)),
    worker(Queue, Parent).

paralell_test() ->
    {ok,P} = equeue:start_link(10),  %%a queue of size 10
    Parent = self(),

    ok = equeue:register_worker(P),
    ok = equeue:active_once(P, ?JOB_TIMEOUT), %%subscribe
    equeue:stop_recv(P), %%desubscribe, check we don't receive anything
    receive
        _X -> ?assert(false)
    after 50 ->
            ok
    end,

    _Senders = lists:foreach(fun(I) -> 
                spawn(fun() ->
                            lists:foreach(fun(J) -> 
                                        Item = {I,J},
                                        equeue:push(P, Item),   %%10 pushes each sender
                                        Parent ! {in, Item}
                                end, lists:seq(1,10))
                    end) 
        end, lists:seq(1,?PARALELL_SENDERS)),  %% 5 senders

    _Receivers = lists:foreach(fun(_) -> 
                spawn(fun() ->
                            ok = equeue:register_worker(P),
                            random:seed(now()),
                            worker(P, Parent)
                    end) end, lists:seq(1,2)),  %% 2 receivers

    All = receive_all(100), 
    ?assertEqual(length(All), 100),
    check_results(All).

check_results(L) ->
    check_results(L, 0).
check_results([], 0) ->
    ok;
check_results([{in,_D}|Rest], N) ->
    ?assert(N =< (10 + ?PARALELL_SENDERS)),  %%because if it is allowed to push 
    check_results(Rest, N+1);
check_results([{out,_D}|Rest], N) ->
    check_results(Rest, N-1);
check_results(A,B) ->
    ?debugFmt("Bad results, got  ~p , ~p \n", [A,B]),
    ?assert(false).

receive_all(0) -> [];
receive_all(N) -> 
    receive
        X ->
            [X | receive_all(N-1)]
    after 200 ->
            []
    end.




worker_crash_test() ->
    {ok,Q} = equeue:start_link(10),
    Parent = self(),
    Pids = lists:map(fun(I) -> spawn(fun() ->
                ok = equeue:register_worker(Q),
                {ok,Item} = equeue:recv(Q, ?JOB_TIMEOUT),
                Parent ! {I, Item}
        end) end, lists:seq(1,5)),
    Pids2 = lists:map(fun(I) -> spawn(fun() ->
                ok = equeue:register_worker(Q),
                equeue:active_once(Q, ?JOB_TIMEOUT),
                receive
                    {job,Item} ->
                        Parent ! {I, Item}
                end
        end) end, lists:seq(1,5)),
    timer:sleep(50), %%give time to the 10 processes do subscribe
    %%then crash some
    exit(lists:nth(1,Pids), crash),  %%crash two processes
    exit(lists:nth(3,Pids), crash),  
    exit(lists:nth(1,Pids2), crash),  %%crash two processes
    exit(lists:nth(3,Pids2), crash),  
    timer:sleep(10),
    equeue:push(Q, a),
    equeue:push(Q, b),
    equeue:push(Q, c),
    equeue:push(Q, d),
    equeue:push(Q, e),
    equeue:push(Q, f),
    Received = receive_all(6),
    ?assertEqual(6, length(Received)),
    ?assertEqual([2,2,4,4,5,5], lists:sort([I || {I,_Item} <- Received])),
    ?assertEqual([a,b,c,d,e,f], lists:sort([Item || {_I,Item} <- Received])),
    ok.


registered_name_test() ->
    Name = 'queue',
    {ok, _} = equeue:start_link(Name, 10),
    ok = equeue:register_worker(Name),
    ok = equeue:push(Name, 'item'),
    ?assertMatch({ok,'item'}, equeue:recv(Name, ?JOB_TIMEOUT)).

    


no_worker_test() ->
    {ok, Q} = equeue:start_link(10),
    ?assertMatch({error, {no_worker_on_queue, Q}}, equeue:push(Q, 'item')).



                
all_worker_crash_test() ->
    {ok,Q} = equeue:start_link(10),
    Pids = lists:map(fun(_I) -> spawn(fun() ->
                ok = equeue:register_worker(Q),
                bad_worker(Q)
        end) end, lists:seq(1,5)),
    timer:sleep(50),
    [exit(Pid, crash) || Pid <- Pids],
    timer:sleep(50),
    ?assertMatch({error, {no_worker_on_queue, Q}}, equeue:push(Q, item)).



bad_worker(Queue ) ->
    ok = equeue:active_once(Queue, ?JOB_TIMEOUT),
    receive
        {job, _} -> ok
    end,
    receive
        this_never_will_match -> ok
    end,
    bad_worker(Queue).

worker_not_registered_test() ->
    {ok,Q} = equeue:start_link(10),
    ?assertEqual({error, no_registered_as_worker}, equeue:recv(Q, ?JOB_TIMEOUT)).

worker_stuck_test() ->
    {ok,Q} = equeue:start_link(10),
    Pids = lists:map(fun(_I) -> spawn(fun() ->
                            ok = equeue:register_worker(Q),
                            bad_worker(Q)
        end) end, lists:seq(1,5)),
    [monitor(process, Pid) || Pid <- Pids],
    equeue:push(Q, a),
    equeue:push(Q, b),
    equeue:push(Q, c),
    equeue:push(Q, d),
    equeue:push(Q, e),
    equeue:push(Q, f),
    timer:sleep(500), %%give time to the queue to kill bad workers
    Down = receive_all(5),
    DownPids = lists:sort([Pid ||  {'DOWN', _, process, Pid, killed} <- Down]),
    ?assertMatch(DownPids, lists:sort(Pids)),
    ?assertMatch({error, {no_worker_on_queue, Q}}, equeue:push(Q, item)).


get_state_test() ->
    {ok,Q} = equeue:start_link(10),
    _Pids = lists:map(fun(_I) -> spawn(fun() ->
                            ok = equeue:register_worker(Q),
                            bad_worker(Q)
        end) end, lists:seq(1,5)),
    timer:sleep(10), %%give time to the listeners to subscribe
    equeue:push(Q, a),
    equeue:push(Q, b),
    timer:sleep(50), %%give time to the queue to deliver jobs
    {ok,R} = equeue:get_state(Q),
    ?assertEqual(10, proplists:get_value(max_size, R)),
    ?assertEqual(0, proplists:get_value(current_size, R) ),
    ?assertEqual(0, proplists:get_value(blocked_senders, R)),
    ?assertEqual(5, proplists:get_value(registered_listeners, R)),
    ?assertEqual(3, proplists:get_value(blocked_listeners, R)).

get_state_blocked_test() ->
    {ok,Q} = equeue:start_link(10),
    _Pids = lists:map(fun(_I) -> spawn(fun() ->
                            ok = equeue:register_worker(Q),
                            bad_worker(Q)
        end) end, lists:seq(1,2)), %%just two listeners
    timer:sleep(10), %%give time to the listeners to subscribe
    _PushersPids = lists:map(fun(_I) -> spawn(fun() ->
                            ok = equeue:push(Q, item)
        end) end, lists:seq(1,20)), %%20 paralell pushes
    timer:sleep(10), %%give time to the pushers
    {ok,R} = equeue:get_state(Q),
    ?assertEqual(10, proplists:get_value(max_size, R)),
    ?assertEqual(18, proplists:get_value(current_size, R) ),
    ?assertEqual(8, proplists:get_value(blocked_senders, R)),
    ?assertEqual(2, proplists:get_value(registered_listeners, R)),
    ?assertEqual(0, proplists:get_value(blocked_listeners, R)).

%% ugly test,  latter make it more clear
request_mark_test() ->
    {ok,Q} = equeue:start_link(10),
%    dbg:tracer(),
%    dbg:p(Q,c),
%    dbg:tpl(equeue, []),
    Self = self(),
    _Pid = spawn(fun() ->
                            ok = equeue:register_worker(Q),
                            _ = equeue:recv(Q, 1000),
                            timer:sleep(500),
                            _ = equeue:recv(Q, 1000),
                            Self ! {done, now()},
                            _ = equeue:recv(Q, 1000)
                    end),
    timer:sleep(10), %%give time to the listeners to subscribe
    ok = equeue:push(Q, item),
    ok = equeue:push(Q, item),
    ok = equeue:mark_completed(Q),
    timer:sleep(50), %% just enough to let the listener call now() before us
    Now = now(),
    receive
        {done, ListenerDone} ->
            Diff = timer:now_diff(Now, ListenerDone),
            ?assert(Diff > 0)
        after 200 ->
                ?assert(false)
    end.

