%%
%% Simpe demo with TCP/IP
%%


-module(demo_connect).

-export([start/0, start/2, loop/2]).

start() ->
    % there should be some configuration stuff
    Host = "localhost",
    Port = 3000,
    start(Host, Port).

start(Host, Port) ->
    {ok, Socket} = gen_tcp:connect(Host, Port, []),
    loop(Socket, 0).

loop(Socket, N) ->
    Msg = io_lib:format("hello: msg ~p~n", [N]),
    ok = gen_tcp:send(Socket, Msg),
    timer:sleep(1000),
    demo_connect:loop(Socket, N + 1).
