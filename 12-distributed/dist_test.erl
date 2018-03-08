-module(dist_test).

-export([test1/1, test2/1, test3/1]).

test1(From) ->
    io:format("[~p] from: ~p~n", [node(), From]),
    From ! {test, node()}.

test2(From) ->
    error_logger:info_msg("[~p] from: ~p~n", [node(), From]),
    From ! {test, node()}.

test3(From) ->
    net_sup ! {test3,  From},
    From ! {test, node()}.
