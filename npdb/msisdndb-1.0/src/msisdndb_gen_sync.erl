%% ============================================================================
%%  Resynchronization functions
%% ============================================================================

-module (msisdndb_gen_sync).

-export([replace/2, patch/2, patch/3]).

-define (DEBUG_N, 100000).
-define (IMORT_BATCH_SIZE, 100000).
-define (SET_MOD, gb_sets).

-include("msisdndb_logging.hrl").
-include("ets_interval_db.hrl").

%% ----------------------------------------------------------------------------
%%
%% ----------------------------------------------------------------------------

replace (Type, FileName) ->
  case file_records (Type, FileName) of
    {ok, NewRecords} ->
      ProcMod = case Type of
        rdc -> msisdndb_rdc;
        npfix -> msisdndb_npfix;
        npmob -> msisdndb_npmob;
        extra -> msisdndb_extra
      end,
      ProcMod:delete_all(),
      ?LOG_INFO ("= ~p = inserting ~p records", [Type, length(NewRecords)]),
      insert_loop (ProcMod, Type, NewRecords, ?IMORT_BATCH_SIZE);
    {error, Reason} -> {error, Reason}
  end.

patch (Type, FileName) -> patch (Type, FileName, false).

patch (Type, FileName, DryRun) ->
  case file_records (Type, FileName) of
    {ok, NewRecords} ->
      TabName = case Type of
        rdc   -> msisdndb_rdc;
        npfix -> msisdndb_npfix;
        npmob -> msisdndb_npmob;
        extra -> msisdndb_extra
      end,

      ?LOG_INFO ("= ~p = checking records", [Type]),
      {ToUpdate, ToInsert, OkCount} = check_records(Type, TabName, NewRecords),
      ?LOG_DBG("~w", [{ToUpdate, ToInsert, OkCount}]),

      TabSet = ?SET_MOD:from_list (table_keys(TabName)),
      RecSet = ?SET_MOD:from_list ([record_key (Type, R) || R <- NewRecords]),
      ToDelSet = ?SET_MOD:subtract(TabSet, RecSet),
      ToDelKeys = ?SET_MOD:to_list (ToDelSet),

      ProcMod = case Type of
        rdc -> msisdndb_rdc;
        npfix -> msisdndb_npfix;
        npmob -> msisdndb_npmob;
        extra -> msisdndb_extra
      end,

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

      {InsertCount, UpdateCount, OkCount, DeleteCount};

    {error, Reason} -> {error, Reason}
  end.


%% ----------------------------------------------------------------------------
%%  local helpers
%% ----------------------------------------------------------------------------

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
    ((N rem ?DEBUG_N) == 0) andalso (N > 0) ->
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

  {ok, {M1, M2, OpId}}.

% initialization of Args structure for given type
line_to_record_init (rdc)   -> binary:compile_pattern([<<";">>]);
line_to_record_init (npfix) -> binary:compile_pattern([<<";">>]);
line_to_record_init (npmob) -> binary:compile_pattern([<<";">>]);
line_to_record_init (extra) -> binary:compile_pattern([<<";">>]).

% insert in smaller batches
insert_loop (Mod, Type, List, N) -> insert_loop (Mod, Type, List, N, 0).

insert_loop (_Mod, _Type, [], _N, Acc) -> Acc;
insert_loop (Mod, Type, List, N, Acc) ->
    {Batch, Tail} = head_n(List, N),
     Mod:insert_many (Batch),
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
      (N rem ?IMORT_BATCH_SIZE) == 0 ->
        ?LOG_INFO ("= ~p = deleted: ~p", [Type, N]);
      true -> ok
    end,

    ProcMod:delete(Key),
    delete_loop (ProcMod, Type, Tail, N + 1);
delete_loop (_, Type, [], N) ->
  ?LOG_INFO ("= ~p = deleted: ~p", [Type, N]),
  ok.

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
