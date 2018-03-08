-module(demo9_worker).
-behaviour(gen_server).

-export([start_link/0, stop/0]).
-export([run1/1, run2/1, get_result/0]).
-export([init/1, terminate/2, handle_call/3, handle_cast/2,handle_info/2, code_change/3]).

-record(loop_data, {
         result
         }).

%% ----------------------------------------------------------------------------
%%  O & M functions
%% ----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link ({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:cast(?MODULE, stop).

%% ----------------------------------------------------------------------------
%%  API functions
%% ----------------------------------------------------------------------------

run1 (N) ->
    gen_server:call(?MODULE, {run1, N}).

run2 (N) ->
    gen_server:cast(?MODULE, {run2, N}).

get_result() ->
    gen_server:call(?MODULE, get_result).


%% ----------------------------------------------------------------------------
%%  gen_server callback functions
%% ----------------------------------------------------------------------------

init ([]) ->
    LoopData = #loop_data{},
    {ok, LoopData}.

handle_call ({run1, N}, _From, LoopData) ->
    Reply = 1 / N,
    {reply, Reply, LoopData};

handle_call (get_result, _From, #loop_data{result = Result} = LoopData) ->
    {reply, Result, LoopData}.

handle_cast ({run2, N}, LoopData) ->
    Result = 1 / N,
    {noreply, LoopData#loop_data{result = Result}};

handle_cast (stop, LoopData) ->
    {stop, normal, LoopData}.

handle_info (_Msg, LoopData) ->
    io:format("~p: unexpected message ~p~n", [?MODULE, _Msg]),
    {noreply, LoopData}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate (_Reason, _LoopData) ->
    ok.
