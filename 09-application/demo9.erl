-module(demo9).

-export([start/0,stop/0]).

% start & stop the application
start() ->
    application:start(demo9).

stop() ->
    application:stop(demo9).
