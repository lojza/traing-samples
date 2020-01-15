%% ============================================================================
%%  Resynchronization functions
%% ============================================================================

-module (msisdndb_tab_sync).

-export([resync_replace/2, resync_patch/2, resync_patch/3]).
-export([apply_increments/2, apply_increments/3]).
-export([file_records/2]).

-define (DEBUG_N, 100000).
-define (IMORT_BATCH_SIZE, 100000).
-define (SET_MOD, gb_sets).

-include("msisdndb_logging.hrl").
-include("ets_interval_db.hrl").

%% ----------------------------------------------------------------------------
%%  API functions
%% ----------------------------------------------------------------------------

% Read full resync file with deleting
% all fileas are deleted and table is loaded from scratch
% WARING - table is empty for a while
resync_replace (Type, FileName) ->
  case file_records (Type, FileName) of
    {ok, NewRecords} ->
      ProcMod = db_proc_mod (Type),
      ProcMod:delete_all(),
      ?LOG_INFO ("= ~p = inserting ~p records", [Type, length(NewRecords)]),
      insert_loop (ProcMod, Type, NewRecords, ?IMORT_BATCH_SIZE);
    {error, Reason} -> {error, Reason}
  end.

% Read full resync file, compare it with current  
% table and do only necesseary changes
resync_patch (Type, FileName) -> resync_patch (Type, FileName, false).

resync_patch (Type, FileName, DryRun) ->
  case file_records (Type, FileName) of
    {ok, NewRecords} ->
      TabName = db_tab_name (Type),

      ?LOG_INFO ("= ~p = checking records", [Type]),
      {ToUpdate, ToInsert, OkCount} = check_records(Type, TabName, NewRecords),
      %?LOG_DBG("~w", [{ToUpdate, ToInsert, OkCount}]),

      TabSet = ?SET_MOD:from_list (table_keys(TabName)),
      RecSet = ?SET_MOD:from_list ([record_key (Type, R) || R <- NewRecords]),
      ToDelSet = ?SET_MOD:subtract(TabSet, RecSet),
      ToDelKeys = ?SET_MOD:to_list (ToDelSet),

      ProcMod = db_proc_mod (Type),

      InsertCount = length(ToInsert),
      UpdateCount = length(ToUpdate),
      DeleteCount = length(ToDelKeys),

      ?LOG_INFO ("= ~p = ok count  ~p", [Type, OkCount]),

      ?LOG_INFO ("= ~p = to delete: ~p", [Type, DeleteCount]),
      if (not DryRun) ->
          delete_loop (ProcMod, Type, ToDelKeys);
        true -> ok
      end,

      ?LOG_INFO ("= ~p = to update: ~p", [Type, UpdateCount]),
      if (not DryRun) ->
          insert_loop (ProcMod, Type, ToUpdate, ?IMORT_BATCH_SIZE);
        true -> ok
      end,

      ?LOG_INFO ("= ~p = to insert: ~p", [Type, InsertCount]),
      if (not DryRun) ->
          insert_loop (ProcMod, Type, ToInsert, ?IMORT_BATCH_SIZE);
        true -> ok
      end,

      ?LOG_INFO ("= ~p = done", [Type]),
      {ok, {InsertCount, UpdateCount, OkCount, DeleteCount}};

    {error, Reason} -> {error, Reason}
  end.

% Read detla files and aply changes
apply_increments (Type, Files) ->
  apply_increments (Type, Files, false).

apply_increments (Type, Files, DryRun) ->
  FileType = case Type of
    npmob -> npmob_incr;
    npfix -> npfix_incr
  end,
  case incr_files_records (Type, FileType, Files) of
    {ok, NewRecords} ->
      TabName = db_tab_name (Type),
      ProcMod = db_proc_mod (Type),
      {ToUpdate, ToInsert, OkCount} = check_records(Type, TabName, NewRecords),

      InsertCount = length(ToInsert),
      UpdateCount = length(ToUpdate),

      ?LOG_INFO ("= ~p = ok count  ~p", [Type, OkCount]),

      ?LOG_INFO ("= ~p = to update: ~p", [Type, UpdateCount]),
      if (not DryRun) ->
          insert_loop (ProcMod, Type, ToUpdate, ?IMORT_BATCH_SIZE);
        true -> ok
      end,

      ?LOG_INFO ("= ~p = to insert: ~p", [Type, InsertCount]),
      if (not DryRun) ->
          insert_loop (ProcMod, Type, ToInsert, ?IMORT_BATCH_SIZE);
        true -> ok
      end,

      ?LOG_INFO ("= ~p = done", [Type]),
      {ok, {InsertCount, UpdateCount, OkCount}};

    {error, Reason} -> {error, Reason}
  end.

%% ----------------------------------------------------------------------------
%%  local helpers
%% ----------------------------------------------------------------------------

% translate type of recort to database process name
db_proc_mod (Type) ->
  case Type of
    rdc -> msisdndb_rdc;
    npfix -> msisdndb_npfix;
    npmob -> msisdndb_npmob;
    extra -> msisdndb_extra
  end.

% type to db table name
db_tab_name (Type) ->
  case Type of
    rdc   -> msisdndb_rdc;
    npfix -> msisdndb_npfix;
    npmob -> msisdndb_npmob;
    extra -> msisdndb_extra
  end.

file_records (Type, FileName) ->
  ?LOG_INFO ("= ~p = loading file ~p", [Type, FileName]),
  case read_file (FileName) of
    {ok, Bin} ->
      LFPatt = binary:compile_pattern(<<"\n">>),
      Args = line_to_record_init (Type),
      Records = lines_to_records (Bin, LFPatt, Type, Args),
      {ok, Records};
    {error, Reason} -> {error, Reason}
  end.

read_file (FileName) ->
  case file:read_file (FileName) of
    {ok, B} ->
      Bin = case filename:extension(FileName) of
        ".gz" -> zlib:gunzip(B);
        _Any -> B
      end,
      {ok, Bin};
    {error, Reason} -> {error, Reason}
  end.

lines_to_records (Bin, LFPatt, Type, Args) ->
  lines_to_records (Bin, LFPatt, Type, Args, 0).

% last empty line
lines_to_records (<<>>, _LFPatt, Type, _Args, N) ->
  ?LOG_INFO ("= ~p = loading ~p done", [Type, N]),
  [];

% loop around lines
lines_to_records (Bin, LFPatt, Type, Args, N) ->
  if
    ((N rem ?DEBUG_N) == 0) andalso (N > 0)  ->
      ?LOG_INFO ("= ~p = loading ~p", [Type, N]);
    true -> ok
  end,

  case binary:split(Bin, LFPatt) of
    [Line, Rest] ->
      case line_to_record (Type, Line, Args, N) of
        skip_line -> lines_to_records (Rest, LFPatt, Type, Args, N + 1);
        {ok, Record} ->
          [ Record | lines_to_records (Rest, LFPatt, Type, Args, N + 1) ]
      end
  end.

% convert one line to the record
line_to_record (rdc, BinLine, Patt, _N) ->
  Fields = binary:split(BinLine, Patt, [global]),
  M1 = erlang:binary_to_integer(lists:nth(2,Fields)),
  M2 = erlang:binary_to_integer(lists:nth(3,Fields)),
  Id1 =  lists:nth(7,Fields),
  Id2 = lists:nth(8,Fields),

  OpId = case Id1 of
    <<"">> -> erlang:binary_to_integer(Id2);
    _ -> erlang:binary_to_integer(Id1)
  end,

  {ok, {M1, M2, OpId}};

line_to_record (npfix, BinLine, Patt, _N) ->
  Fields = binary:split(BinLine, Patt, [global]),
  M1 = erlang:binary_to_integer(lists:nth(1,Fields)),
  M2s = lists:nth(2,Fields),

  M2 = case M2s of
    <<"">> -> M1;
    _ -> erlang:binary_to_integer(M2s)
  end,
            
  OpId = erlang:binary_to_integer(lists:nth(3,Fields)),            

  {ok, {M1, M2, OpId}};

line_to_record (npmob, _BinLine, _Patt, 0) ->
  skip_line;
line_to_record (npmob, BinLine, Patt, _N) ->
  Fields = binary:split(BinLine, Patt, [global]),
  Msisdn = erlang:binary_to_integer(lists:nth(1,Fields)),
  OpId = erlang:binary_to_integer(lists:nth(3,Fields)),
  {ok, {Msisdn, OpId}};
    
line_to_record (extra, BinLine, Patt, _N) ->
  Fields = binary:split(BinLine, Patt, [global]),
  M1 = erlang:binary_to_integer(lists:nth(1,Fields)),
  M2s = lists:nth(2,Fields),

  M2 = case M2s of
    <<"">> -> M1;
    _ -> erlang:binary_to_integer(M2s)
  end,

  OpId = erlang:binary_to_integer(lists:nth(3,Fields)),

  {ok, {M1, M2, OpId}};

line_to_record (npmob_incr, BinLine, Patt, _N) ->
  Fields = binary:split(BinLine, Patt, [global]),
  M = erlang:binary_to_integer(lists:nth(1,Fields)),
  OpId = erlang:binary_to_integer(lists:nth(4,Fields)),
  {ok, {M, OpId}};

line_to_record (npfix_incr, BinLine, Patt, _N) ->
  Fields = binary:split(BinLine, Patt, [global]),
  M1 = erlang:binary_to_integer(lists:nth(1,Fields)),
  M2s = lists:nth(2,Fields),

  M2 = case M2s of
    <<"">> -> M1;
    _ -> erlang:binary_to_integer(M2s)
  end,

  OpId = erlang:binary_to_integer(lists:nth(4,Fields)),

  {ok, {M1, M2, OpId}}.

% initialization of Args structure for given type
line_to_record_init (rdc)   -> binary:compile_pattern([<<";">>]);
line_to_record_init (npfix) -> binary:compile_pattern([<<";">>]);
line_to_record_init (npmob) -> binary:compile_pattern([<<";">>]);
line_to_record_init (extra) -> binary:compile_pattern([<<";">>]);
line_to_record_init (npmob_incr) -> binary:compile_pattern([<<"|">>]);
line_to_record_init (npfix_incr) -> binary:compile_pattern([<<"|">>]).

% insert in smaller batches
insert_loop (Mod, Type, List, N) -> insert_loop (Mod, Type, List, N, 0).

insert_loop (_Mod, _Type, [], _N, Acc) -> Acc;
insert_loop (Mod, Type, List, N, Acc) ->
    {Batch, Tail} = head_n(List, N),
    _R = Mod:insert_many (Batch),
    %?LOG_DBG ("R: ~p", [_R]),
    Count = length (Batch),
    NewAcc = Acc + Count,
    ?LOG_INFO ("= ~p = inserted: ~p", [Type, NewAcc]),
    insert_loop (Mod, Type, Tail, N, NewAcc).

% split list at N-th item
head_n (List, N) ->
    head_n (List, N, []).

head_n ([H | T], N, Acc) when N > 0 ->
%io:format("~p~n", [N]),
    head_n (T, N - 1, [H | Acc]);
head_n (T, 0, Acc) -> {lists:reverse(Acc), T};
head_n ([], _N, Acc) -> {lists:reverse(Acc), []}.

delete_loop (ProcMod, Type, List) -> delete_loop (ProcMod, Type, List, 0).

delete_loop (ProcMod, Type, [Key | Tail], N) ->
    if 
      ((N rem ?IMORT_BATCH_SIZE) == 0) andalso (N > 0) ->
        ?LOG_INFO ("= ~p = deleted: ~p", [Type, N]);
      true -> ok
    end,

    ProcMod:delete(Key),
    delete_loop (ProcMod, Type, Tail, N + 1);
delete_loop (_, Type, [], N) ->
  if
    (N > 0) -> 
      ?LOG_INFO ("= ~p = deleted: ~p done", [Type, N]);
    true -> ok
  end.

table_keys(TabName) ->
  Key = ets:first(TabName),
  table_keys (TabName, Key, []).

table_keys (_, '$end_of_table', Acc) -> Acc;
table_keys (TabName, Key, Acc) ->
  NextKey = ets:next(TabName, Key),
  table_keys (TabName, NextKey, [Key | Acc]).

check_records(Type, Tab, NewRecords) ->
  check_records(Type, Tab, NewRecords, [], [], 0).

check_records(Type, Tab, [Record | Tail], UpdateAcc, InsertAcc, OkCount) ->
  Key = record_key (Type, Record),

  case ets:lookup (Tab, Key) of
    [Item] ->
      case is_equal (Type, Record, Item) of
        true  -> check_records (Type, Tab, Tail, UpdateAcc, InsertAcc, OkCount + 1);
        false -> check_records (Type, Tab, Tail, [Record | UpdateAcc], InsertAcc, OkCount)
      end;
    [] -> check_records (Type, Tab, Tail, UpdateAcc, [Record | InsertAcc], OkCount)
  end;
check_records(_Type, _Tab, [], UpdateAcc, InsertAcc, OkCount) -> {UpdateAcc, InsertAcc, OkCount}.


is_equal (_Type, {From,To,Value}, #interval{from_number = IFrom, to_number = ITo, data = IValue}) ->
  {From,To,Value} == {IFrom, ITo, IValue};
is_equal (_Type, A, B) -> A == B.

record_key (Type, Record) ->
  case Type of
    rdc   -> element(1, Record);
    npmob -> element(1, Record);
    npfix -> element(1, Record);
    extra -> element(1, Record)
  end.

incr_files_records (Type, FileType, Files) ->
  incr_files_records (Type, FileType, Files, []).

incr_files_records (Type, FileType, [File | Tail], Acc) ->
  case file_records (FileType, File) of
    {ok, Records} ->
      incr_files_records (Type, FileType, Tail, [Records | Acc]);
    {error, Reason} -> {error, Reason}
  end;
incr_files_records (Type, _FileType, [], Acc) -> 
  ListOfBatches = lists:reverse(Acc),
  {ok, incr_merge_records(Type, ListOfBatches)}.


incr_merge_records (Type, ListOfBatches) ->
  case ListOfBatches of
    [ First | Tail ] ->
      KeyList = [{record_key (Type, Item), Item}  || Item <- First],
      OrdDict = orddict:from_list(KeyList),
      ResultTree = incr_merge_records (Type, Tail, gb_trees:from_orddict(OrdDict)),
      ResultList = gb_trees:to_list(ResultTree),
      [ Val || {_, Val} <- ResultList ];
    [] -> []
  end.

incr_merge_records (Type, [Batch | Tail], Tree) ->
  KeyList = [{record_key (Type, Item), Item}  || Item <- Batch],
  Tree1 = gb_trees_enter_many (KeyList, Tree),
  incr_merge_records (Type, Tail, Tree1);
incr_merge_records (_Type, [], Tree) -> Tree.

gb_trees_enter_many ([{Key, Value} | Tail], Tree) ->
  NewTree = gb_trees:enter(Key, Value, Tree),
  gb_trees_enter_many (Tail, NewTree);
gb_trees_enter_many ([], Tree) -> Tree.
