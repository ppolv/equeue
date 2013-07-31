-module(equeue_test).

-include_lib("eunit/include/eunit.hrl").

-define(PARALELL_SENDERS,5).

blocking_worker(Queue, Parent) ->
    {ok, J} = equeue:recv(Queue),
    Parent ! {out, J},
    blocking_worker(Queue, Parent).

blocking_test() ->
    {ok,Q} = equeue:start_link(10),  %%a queue of size 10
    Parent = self(),
    lists:foreach(fun(_) ->
                spawn(fun() -> blocking_worker(Q, Parent) end)
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
    equeue:active_once(Queue),
    receive
        X ->
            Parent ! {out,X}
    end,
    timer:sleep(random:uniform(20)),
    worker(Queue, Parent).

paralell_test() ->
    {ok,P} = equeue:start_link(10),  %%a queue of size 10
    Parent = self(),

    ok = equeue:active_once(P), %%subscribe
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
    after 100 ->
            []
    end.




worker_crash_test() ->
    {ok,Q} = equeue:start_link(10),
    Parent = self(),
    Pids = lists:map(fun(I) -> spawn(fun() ->
                {ok,Item} = equeue:recv(Q),
                Parent ! {I, Item}
        end) end, lists:seq(1,5)),
    Pids2 = lists:map(fun(I) -> spawn(fun() ->
                equeue:active_once(Q),
                receive
                    {job,Item} ->
                        Parent ! {I, Item}
                end
        end) end, lists:seq(1,5)),
    timer:sleep(100), %%give time to the 10 processes do subscribe
    %%then crash some
    exit(lists:nth(1,Pids), crash),  %%crash two processes
    exit(lists:nth(3,Pids), crash),  
    exit(lists:nth(1,Pids2), crash),  %%crash two processes
    exit(lists:nth(3,Pids2), crash),  
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
    ok = equeue:push(Name, 'item'),
    ?assertMatch({ok,'item'}, equeue:recv(Name)).

    




                
