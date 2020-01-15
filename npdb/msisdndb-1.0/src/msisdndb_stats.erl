%% ===========================================================================
%%  Statistics counter module
%% ===========================================================================

-module (msisdndb_stats).

-export([start_link/0, stop/0]).
-export([increment/1, lookup/1, reset/1, reset_all/0]).
-export([init/1, terminate/2, handle_call/3, handle_cast/2,handle_info/2, code_change/3]).

-behavior(gen_server).

-record (loop_data, {}).

-define (COUNTER_TAB_NAME, ?MODULE).
-define (MAX_COUNTER, 16#ffffffff). % four bytes

-include("msisdndb_logging.hrl").

%% ---------------------------------------------------------------------------
%%  Operation & Mainenance
%% ---------------------------------------------------------------------------
start_link() ->
    gen_server:start_link ({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:cast(?MODULE, stop).

%% ---------------------------------------------------------------------------
%%  API function
%% ---------------------------------------------------------------------------
increment (Counter) ->
    gen_server:cast(?MODULE, {increment, Counter}).

reset (Counter) ->
    gen_server:call(?MODULE, {reset, Counter}).

reset_all () ->
    gen_server:call(?MODULE, reset_all).

lookup (Counter) ->
    case ets:lookup(?COUNTER_TAB_NAME, Counter) of
        [{Counter, N}] -> N;
        [] -> 0
    end.

%%% ---------------------------------------------------------------------------
%%%  gen_server callbacks
%%% ---------------------------------------------------------------------------

init ([]) ->
    erlang:process_flag(trap_exit, true),
    ets:new (?COUNTER_TAB_NAME, [set, named_table, {keypos, 1}]),
    LoopData = #loop_data{},
    {ok, LoopData}.

terminate (_Reason, _LoopData) ->
    ets:delete (?COUNTER_TAB_NAME),
    ok.

handle_cast ({increment, Counter}, LoopData) ->
    case ets:lookup(?COUNTER_TAB_NAME, Counter) of
      [_] -> ets:update_counter(?COUNTER_TAB_NAME, Counter, {2, 1, ?MAX_COUNTER, 1});
      [] -> ets:insert (?COUNTER_TAB_NAME, {Counter, 1})
    end,
    {noreply, LoopData};

handle_cast (stop, LoopData) ->
    {stop, normal, LoopData};

handle_cast (Request, LoopData) ->
    ?LOG_ERROR("~p recieved unexpected cast: ~p", [?MODULE, Request]),
    {noreply, LoopData}.

handle_call ({reset, Counter}, _From, LoopData) ->
    Reply = ets:delete (?COUNTER_TAB_NAME, Counter),
    {reply, Reply, LoopData};

handle_call (reset_all, _From, LoopData) ->
    Reply = ets:delete_all_objects (?COUNTER_TAB_NAME),
    {reply, Reply, LoopData}.

handle_info (Msg, LoopData) ->
    ?LOG_ERROR("~p recieved unexpected messange: ~p", [?MODULE, Msg]),
    {noreply, LoopData}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
