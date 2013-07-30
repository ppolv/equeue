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
    check_results(Rest, N-1).

receive_all(0) -> [];
receive_all(N) -> 
    receive
        X ->
            [X | receive_all(N-1)]
    end.



