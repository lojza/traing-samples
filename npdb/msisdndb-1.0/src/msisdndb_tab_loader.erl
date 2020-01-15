%% ============================================================================
%%  Table loader - find relvatn files, check them and perform changes
%%  if necessary.
%% ============================================================================

-module(msisdndb_tab_loader).
-behaviour(gen_server).

-export([start_link/2, start_link/3, stop/1]).
-export([init/1, terminate/2, handle_call/3, handle_cast/2,handle_info/2, code_change/3]).

-export([reload/1, find_relevant_files/3, all_keys/1, load_info/1]).

-record (loop_data, {
  type,
  data_path_full,
  data_path_incr,
  mod_cb,
  load_info_file,
  load_info,
  timer_ref
}).

-record (db_file_info, {
  path,
  size,
  mtime
}).


-include_lib("kernel/include/file.hrl").
-include("msisdndb_logging.hrl").

-define(DEFAULT_RELOAD_TICK, 10000).


%% ----------------------------------------------------------------------------
%%  O & M functions
%% ----------------------------------------------------------------------------

start_link (ProcName, Type) ->
  start_link (ProcName, Type, []).

start_link (ProcName, Type, Opts) ->
  gen_server:start_link ({local, ProcName}, ?MODULE, [Type, Opts], []).

stop(ProcRef) ->
  gen_server:cast(ProcRef, stop).


%% ----------------------------------------------------------------------------
%%  API functions
%% ----------------------------------------------------------------------------

reload(ProcName) ->
  gen_server:call(ProcName, reload).

load_info (ProcName) ->
  gen_server:call(ProcName, load_info).


%% ----------------------------------------------------------------------------
%%  gen_server callbacks
%% ----------------------------------------------------------------------------

init ([Type, Opts]) ->
  {ModCb, PathKeyFull, PathKeyIncr, LoadInfoFN} = case Type of
    npmob -> {msisdndb_loader_cbmob, input_mob_full, input_mob_incr, "loadinfo_npmob.txt"};
    npfix -> {msisdndb_loader_cbfix, input_fix_full, input_fix_incr, "loadinfo_npfix.txt"};
    rdc   -> {msisdndb_loader_cbrdc, input_rdc_full, undefined, "loadinfo_rdc.txt"};
    extra -> {msisdndb_loader_cbextra, input_extra_full, undefined, "loadinfo_extra.txt"}
  end,

  DataPathFull = case msisdndb_cfg_util:input_path (PathKeyFull, Opts) of
    {ok, PF} -> PF;
    undefined -> error({"input data path for full is not set", Type, PathKeyFull})
  end,

  DataPathIncr = case msisdndb_cfg_util:input_path (PathKeyIncr, Opts) of
    {ok, PI} -> PI;
    undefined -> undefined
  end,

  LoadInfoFile = case msisdndb_cfg_util:input_path (load_file_info_path, Opts) of
    {ok, PIF} -> filename:join(PIF, LoadInfoFN);
    undefined -> error({"input data path for load info files is not set", load_file_info_path})
  end,

  LoadInfo = case orddict_load(LoadInfoFile) of
    {ok, Od} -> Od;
    {error,enoent} -> undefined
  end,

  ReoladTickMs = proplists:get_value(reolad_tick, Opts, ?DEFAULT_RELOAD_TICK),
  {ok, TRef} = timer:apply_interval(ReoladTickMs, ?MODULE, reload, [self()]),

  LoopData = #loop_data{
      type = Type,
      data_path_full = DataPathFull,
      data_path_incr = DataPathIncr,
      load_info_file = LoadInfoFile,
      load_info = LoadInfo,
      mod_cb = ModCb,
      timer_ref = TRef},
  {ok, LoopData}.

handle_cast (stop, LoopData) ->
  {stop, normal, LoopData}.

handle_call (reload, _From, #loop_data{type = Type} = LoopData) ->
  LoadInfo = LoopData#loop_data.load_info,
  LoadInfoFile = LoopData#loop_data.load_info_file,
  DataPathFull = LoopData#loop_data.data_path_full,
  DataPathIncr = LoopData#loop_data.data_path_incr,
  ModCb = LoopData#loop_data.mod_cb,
  NewLoadInfo = find_relevant_files(DataPathFull, DataPathIncr, ModCb),

  Reply = case load_info_diff (LoadInfo, NewLoadInfo) of
    no_changes -> no_changes;
    {replace, NewLoadInfo} ->
      {_, _, #db_file_info{path = FP}} = orddict:fetch (last_full, NewLoadInfo),
      Increments = orddict:fetch (increments, NewLoadInfo),
      IPLst = [ P || {_, _, #db_file_info{path = P}} <- Increments ],
      R1 = msisdndb_tab_sync:resync_patch (Type, FP),
      R2 = if
        IPLst /= [] -> msisdndb_tab_sync:apply_increments (Type, IPLst);
        IPLst == [] -> []
      end,
      {replace, R1, R2};
    {append, Increments} ->
      IPLst = [ P || {_, _, #db_file_info{path = P}} <- Increments ],
      R = msisdndb_tab_sync:apply_increments (Type, IPLst),
      {append, R}
  end,

  if
    NewLoadInfo /= LoadInfo ->
      orddict_save (LoadInfoFile, NewLoadInfo),
      {reply, Reply, LoopData#loop_data{load_info = NewLoadInfo}};
    true ->
      {reply, Reply, LoopData}
  end;

handle_call (load_info, _From, LoopData) ->
  LoadInfo = LoopData#loop_data.load_info,
  {reply, orddict:to_list(LoadInfo), LoopData}.

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

find_relevant_files (PathFull, PathIncr, ModCb) ->
  St = ModCb:init(),
  FilesFull = files_from_path (PathFull),
  FilesIncr = if
    PathIncr == undefined -> [];
    true -> files_from_path (PathIncr)
  end,

  InfoFull = find_files (full, FilesFull, ModCb, St),
  InfoIncr = find_files (increment, FilesIncr, ModCb, St),

  % find latest increment
  LastFull = lists:max([ {Time, P, Info} || {Time, P, Info} <- InfoFull ]),
  {FullTime, _, _} = LastFull,
  Increments = lists:sort([ {Time, P, Info}
      || {Time, P, Info} <- InfoIncr, (Time > FullTime) and (Info#db_file_info.size > 0)]),
  orddict:from_list([{last_full, LastFull}, {increments, Increments}]).

find_files (FileType, [Path | Tail], ModCb, ModCbSt) ->
  Name = filename:basename(Path),
  case ModCb:file_type (Name, ModCbSt) of
    {FT, Time} when FT == FileType ->
      Info = file_info (Path),
      [ {Time, Name, Info} | find_files (FileType, Tail, ModCb, ModCbSt) ];
    Result ->
      Cause = case Result of
        {T, _} -> T;
        nomatch -> nomatch
      end,
      ?LOG_WARN("unexpected file ~p when was looking for file type ~p - skipping ~p", [Name, FileType, Cause]),
      find_files (FileType, Tail, ModCb, ModCbSt)
  end;
find_files (_FileType, [], _ModCb, _ModCbSt) -> [].


file_info (Path) ->
  {ok, #file_info{size = Size, mtime = MTime}} = file:read_file_info (Path),
  #db_file_info{path = Path, size = Size, mtime = MTime}.

all_keys (TabName) ->
  First = ets:first(TabName),
  all_keys_loop(First, TabName).

all_keys_loop('$end_of_table', _TabName) -> [];
all_keys_loop(Item, TabName) ->
  Next = ets:next(TabName, Item),
  [Item | all_keys_loop(Next, TabName)].

orddict_save (FileName, OrdDict) ->
  file:write_file(FileName, io_lib:format("% === this file is generated ===~n~n~p.~n", [orddict:to_list(OrdDict)])).

orddict_load (FileName) ->
  case file:consult(FileName) of
    {ok, [Data]} -> {ok, orddict:from_list(Data)};
    {ok, _} -> {error, invalid_format};
    {error, Reason} -> {error, Reason}
  end.

load_info_diff (OldInfo, NewInfo) when OldInfo == NewInfo -> no_changes;
load_info_diff (undefined, NewInfo) -> {replace, NewInfo};
load_info_diff (OldInfo, NewInfo) ->
  OldFull = orddict:fetch (last_full, OldInfo),
  NewFull = orddict:fetch (last_full, NewInfo),
  case is_file_info_equal(OldFull, NewFull) of
    false -> {replace, NewInfo};
    true  ->
      OldIncrements = orddict:fetch (increments, OldInfo),
      NewIncrements = orddict:fetch (increments, NewInfo),
      case skip_common_increments (OldIncrements, NewIncrements) of
        {[], []} -> no_changes;
        {[], NewRest} -> {append, NewRest};
        {_, _} -> {replace, NewInfo}
      end
  end.

is_file_info_equal (I, I) -> true;
is_file_info_equal ({Time, Name, FiOld}, {Time, Name, FiNew}) ->
  (FiOld#db_file_info.size == FiNew#db_file_info.size) andalso
  (FiOld#db_file_info.mtime == FiNew#db_file_info.mtime);
is_file_info_equal (_, _) -> false.

skip_common_increments ([I1 | OldTail], [I2 | NewTail]) when I1 == I2 ->
  skip_common_increments (OldTail, NewTail);
skip_common_increments ([OldInr | OldTail] = OldRest, [NewInr | NewTail] = NewRest) ->
  case is_file_info_equal (OldInr, NewInr) of
    true  -> skip_common_increments (OldTail, NewTail);
    false -> {OldRest, NewRest}
  end;
skip_common_increments (OldRest, NewRest) -> {OldRest, NewRest}.
