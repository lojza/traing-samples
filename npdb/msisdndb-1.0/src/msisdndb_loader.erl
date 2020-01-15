%% ============================================================================
%%  Simple loader for NP database
%% ============================================================================
-module(msisdndb_loader).
-behaviour(gen_server).

-export([start_link/3, stop/1]).
-export([reload/1]).
-export([init/1, terminate/2, handle_call/3, handle_cast/2,handle_info/2, code_change/3]).

-export([find_relevant_files/2, all_keys/1]).

-record (loop_data, {
  data_path,
  mod_cb,
  last_full,
  last_incr
}).

-include_lib("kernel/include/file.hrl").
-include("msisdndb_logging.hrl").


%% ----------------------------------------------------------------------------
%%  O & M functions
%% ----------------------------------------------------------------------------

start_link (ProcName, DataPath, ModCb) ->
  gen_server:start_link ({local, ProcName}, ?MODULE, [DataPath, ModCb], []).

stop(ProcRef) ->
  gen_server:cast(ProcRef, stop).


%% ----------------------------------------------------------------------------
%%  API functions
%% ----------------------------------------------------------------------------

reload(ProcName) ->
  gen_server:cast(ProcName, reload).


%% ----------------------------------------------------------------------------
%%  gen_server callbacks
%% ----------------------------------------------------------------------------

init ([DataPath, ModCb]) ->
  LoopData = #loop_data{data_path = DataPath, mod_cb = ModCb},
  {ok, LoopData}.

handle_cast (stop, LoopData) ->
  {stop, normal, LoopData}.

handle_call (reload, _From, LoopData) ->
  Reply = ok,
  {reply, Reply, LoopData}.

handle_info (Msg, LoopData) ->
  ?LOG_ERROR("~p recieved unexpected messange: ~p", [?MODULE, Msg]),
  {noreply, LoopData}.

terminate (_Reason, _LoopData) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


%% ----------------------------------------------------------------------------
%%  local helpers
%% ----------------------------------------------------------------------------

files_from_path (Path) ->
  filelib:wildcard(Path ++ "/*").

find_relevant_files (Path, ModCb) ->
  St = ModCb:init(),
  Files = files_from_path (Path),
  Types = [ 
    begin 
      N = filename:basename(P),
      T = ModCb:file_type (N, St),
      Info = file_info (P),
      {T, P, Info}
    end || P <- Files ],

  % find latest increment
  LastFull = lists:max([ {Time, P, Info} || {{full, Time}, P, Info = {_, FileSize}} <- Types, FileSize > 0  ]),
  {FullTime, _, _} = LastFull,
  Increments = lists:sort([ {Time, P, Info} 
      || {{increment, Time}, P, Info = {_, FileSize}} <- Types, (Time > FullTime) and (FileSize > 0)]),
  orddict:from_list([{last_full, LastFull}, {increments, Increments}]).

file_info (Path) ->
  {ok, #file_info{size = Size, mtime = MTime}} = file:read_file_info (Path),
  {MTime, Size}.

all_keys (TabName) ->
  First = ets:first(TabName),
  all_keys_loop(First, TabName).

all_keys_loop('$end_of_table', _TabName) -> [];
all_keys_loop(Item, TabName) -> 
  Next = ets:next(TabName, Item),
  [Item | all_keys_loop(Next, TabName)].
