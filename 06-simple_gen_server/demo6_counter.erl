%% ============================================================================
%%  Ukazka pouziti gen_server
%%
%% ============================================================================
-module(demo6_counter).
-behaviour(gen_server).

-export([start_link/1, stop/0]).
-export([increment/0, increment/1, get_count/0, reset/0]).
-export([init/1, terminate/2, handle_call/3, handle_cast/2,handle_info/2, code_change/3]).

-record(loop_data, {
    max,
    count
}).

%% ----------------------------------------------------------------------------
%%  O & M functions
%% ----------------------------------------------------------------------------

start_link(MaxValue) ->
    gen_server:start_link ({local, ?MODULE}, ?MODULE, [MaxValue], []).

stop() ->
    gen_server:cast(?MODULE, stop).

%% ----------------------------------------------------------------------------
%%  API functions
%% ----------------------------------------------------------------------------

increment () -> increment (1).

increment (N) ->
    gen_server:cast(?MODULE, {increment, N}).

get_count() ->
    gen_server:call(?MODULE, get_count).

reset() ->
    gen_server:call(?MODULE, reset).


%% ----------------------------------------------------------------------------
%%  gen_server callback functions
%% ----------------------------------------------------------------------------

init ([Max]) ->
    LoopData = #loop_data{max = Max, count = 0},
    {ok, LoopData}.

handle_call (get_count, _From, #loop_data{count = Count} = LoopData) ->
    Reply = {ok, Count},
    {reply, Reply, LoopData};

handle_call (reset, _From, #loop_data{} = LoopData) ->
    Reply = ok,
    {reply, Reply, LoopData#loop_data{count = 0}}.

handle_cast ({increment, N}, #loop_data{count = Count, max = Max} = LoopData) ->
    NewCount = increment_max (Count, N, Max),
    {noreply, LoopData#loop_data{count = NewCount}};

handle_cast (stop, LoopData) ->
    {stop, normal, LoopData}.

handle_info (_Msg, LoopData) ->
    io:format("~p: unexpected message ~p~n", [?MODULE, _Msg]),
    {noreply, LoopData}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate (_Reason, _LoopData) ->
    ok.

%% ----------------------------------------------------------------------------
%%  local functions
%% ----------------------------------------------------------------------------

increment_max (Count, N, Max) ->
    NewCount = Count + N,
    if NewCount =< Max -> NewCount;
       NewCount >  Max -> 0
    end.
