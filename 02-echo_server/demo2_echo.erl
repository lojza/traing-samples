%%
%% {ok, Pid} = demo2_echo:start().
%% demo2_echo:echo(Pid, "12345").
%% demo2_echo:stop(Pid).
%% demo2_echo:echo(Pid, "12345").
%%

-module(demo2_echo).

-export([start/0, stop/1, echo/2]).
-export([init/0, loop/0]).

%%
%% Operation and maitenece functions
%%

start() ->
    Pid = spawn(?MODULE, init, []),
    {ok, Pid}.

stop (Pid) ->
    Pid ! stop.

%%
%% API functions
%%

echo (ProcRef, Data) ->
   ProcRef ! {echo, self(), Data},
   receive
       {reply, Reply} -> Reply
   after 500 -> timeout
   end.

%%
%% internal functions
%%

init() ->
   loop ().

loop () ->
   receive
       {echo, From, Data} ->
           From ! {reply, Data},
           loop ();
       stop -> ok
   end.   
