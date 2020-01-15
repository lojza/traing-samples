%% ============================================================================
%%   Non overlapping intervals storage and lookup database in ETS tables.
%% ============================================================================
-module(ets_interval_db).

-export ([new/1, insert/4, insert_many/2, lookup/2, delete/3, delete/2, delete/1, delete_all/1, delete_by_key/2]).
-export ([keypos/0]).
-export ([check_new_values/1]).

-include_lib("stdlib/include/ms_transform.hrl").

-include("ets_interval_db.hrl").

-include_lib("eunit/include/eunit.hrl").

-define (USE_OLD_CHECK, true).

%% ----------------------------------------------------------------------------
%%   New table - create underlying ETS table.
%%   It is an ordered set with one record for every interval.
%%   Beginning of the interval is an indexed key.
%% ----------------------------------------------------------------------------
new(TabName) ->
  ets:new(TabName, [ordered_set, named_table, {keypos, #interval.from_number}]).

keypos() -> #interval.from_number.

%% ----------------------------------------------------------------------------
%%   Insert new interval to the database.
%%   New interval cannot overlap existing intervals.
%%     TabId      - which table
%%     FromNumber - start of the interval
%%     ToNumber   - end of the interval
%%     Data       - data connected wit the interval
%% ----------------------------------------------------------------------------
insert (TabId, FromNumber, ToNumber, Data)  ->
  Interval = #interval{from_number = FromNumber, to_number = ToNumber, data = Data},
  case can_insert (TabId, Interval) of
    ok ->
      ets:insert (TabId, Interval),
      {ok, Interval};
    {error, Reason} -> {error, Reason}
  end.


%% ----------------------------------------------------------------------------
%%   Insert list of new intervals to the table.
%%     TabId    - which table
%%     Values   - list of tuples {FromNumber, ToNumber, Data}
%% ----------------------------------------------------------------------------
insert_many (TabId, Values) ->
  Intervals = [
    #interval{from_number = FromNumber, to_number = ToNumber, data = Data} ||
    {FromNumber, ToNumber, Data} <- Values
  ],

  case check_new_values (Intervals) of
    ok ->
      case can_insert_many (TabId, Intervals) of
        ok ->
          ets:insert (TabId, Intervals),
          {ok, Intervals};
        {error, Reason} -> {error, Reason}
      end;
    {error, Reason} -> {error, Reason}
  end.


%% ----------------------------------------------------------------------------
%%   Find an interval for a point.
%%     TabId - which table
%%     Number - tested point
%% ----------------------------------------------------------------------------
lookup (TabId, Number) when is_integer (Number) ->
  % there is needed =<, but ets:prev provides <,
  % A =< B is the same as A < (B + 1) when A and B are itntegers
  % otherwise there should be two lookups. ets:lookup and ets:prev
  SearchKey = Number + 1,
  case ets:prev (TabId, SearchKey) of
    '$end_of_table' -> [];
    PrevKey ->
      case ets:lookup(TabId, PrevKey) of
        [#interval{from_number = From, to_number = To, data = Data}]
            when ((From =< Number) and (Number =< To)) -> [{From, To, Data}];
        [#interval{}] -> []
      end
  end.

%% ----------------------------------------------------------------------------
%%   Delete an interval from the database.
%%     TabId - which table
%%     FromNumber, ToNumber - interval
%% ----------------------------------------------------------------------------
delete (TabId, FromNumber, ToNumber) ->
  case ets:select (TabId, ets:fun2ms (
    fun (#interval{from_number = F, to_number = T} = I)
    when
      ((F == FromNumber) and
       ((T == ToNumber) or (ToNumber == undefined))
      ) -> I
    end
  )) of
    [ItemToDelete] ->
      ets:delete_object(TabId, ItemToDelete),
      {ok, ItemToDelete};
    [] -> {error, instance}
  end.

%% ----------------------------------------------------------------------------
%%   Delete an interval from the database which starts with FromNumber.
%%   ToNumber could not be specified.
%%
%%     TabId - which table
%%     FromNumber - interval
%% ----------------------------------------------------------------------------
delete (TabId, FromNumber) ->
   delete (TabId, FromNumber, undefined).

%% ----------------------------------------------------------------------------
%%   Delete an intervals from the database.
%%
%%     TabId - which table
%% ----------------------------------------------------------------------------
delete_all (TabId) ->
   ets:delete_all_objects (TabId).

%% ----------------------------------------------------------------------------
%%   Detete whole table.
%%     TabId - which table
%% ----------------------------------------------------------------------------
delete (TabId) ->
  ets:delete (TabId).

delete_by_key (TabId, FromKey) ->
  ets:delete(TabId, FromKey).

%% ----------------------------------------------------------------------------
%%   Check if this value can be inserted to the table.
%% ----------------------------------------------------------------------------
can_insert (TabId, #interval{from_number = FromNumber, to_number = ToNumber}) when FromNumber =< ToNumber ->

  % check if from numbers is not in some another interval
  case lookup (TabId, FromNumber) of

    % from number is not in some interval
    [] ->
      % find fitrst interval behind from number and check if it is
      NextFrom = ets:next (TabId, FromNumber),
      if
        % there is no such interval or next interval is far enougth
        ((NextFrom == '$end_of_table') or (ToNumber < NextFrom)) -> ok;
        % next interval overlaps new interval
        true -> {error, {overlap_next, NextFrom, FromNumber, ToNumber}}
      end;

    % interval exists and is the same or bigger
    % new interval will replace it
    [{FromNumber, CITo, _Value}] when CITo >= ToNumber ->
      ok;
    [{FromNumber, CITo, _Value} = CurrInterval] when CITo < ToNumber ->
      % new interval is bigger, we need to check if next interval is far enougth
      NextFrom = ets:next (TabId, FromNumber),
      if
        ((NextFrom == '$end_of_table') or (ToNumber < NextFrom)) ->
          ok;
        true -> {error, {overlap_next2, CurrInterval, FromNumber, ToNumber}}
      end;
    % current interval start before CurrInterval - inpossible to insert
    [CurrInterval] ->
      {error, {overlap_exists, CurrInterval, FromNumber, ToNumber}}
  end.

%% ----------------------------------------------------------------------------
%%   Check if this values (lis of values) can be inserted to the table.
%% ----------------------------------------------------------------------------
can_insert_many (_TabId,  []) -> ok;
can_insert_many (TabId, [Interval | Tail]) ->
  case can_insert (TabId, Interval) of
    ok -> can_insert_many (TabId, Tail);
    {error, Reason} -> {error, Reason}
  end.

%% ----------------------------------------------------------------------------
%%   Validate new valueas (if they are not overlapping)
%% ----------------------------------------------------------------------------

-ifdef(USE_OLD_CHECK).

check_new_values (Intervals) ->

  % Make from interval list of tuples {From, To} which
  % are sorted (using first key).
  Keys = [{F, T} || #interval{from_number = F, to_number = T} <- Intervals],
  KeysSorted = lists:sort(Keys),

  % check if all from number is greater then to number in previous
  % record (interval are not overlaped).
  {CheckResult, _} = lists:foldl (

    % accumulator is {Result, PrevKey}
    fun ({From, To} = Key, {Result, PrevKey}) ->
      case Result of
        ok ->
          if
            % check if FromNumber < ToNumber
            From =< To ->
              case PrevKey of
                % process prev key
                {_PrevFrom, PrevTo} ->

                  % finally check borders
                  if
                    (PrevTo <  From) -> {ok, Key};
                    (PrevTo >= From) -> {{error, {interval_overlap, PrevKey, Key}}, Key}
                  end;

                % first round - nothing to compare
                undefined -> {ok, Key}
              end;

            % bad interval
            From > To -> {{error, {bad_interval, Key}}, Key}
          end;

        % some error in previos recods, do nothing and send it
        % to the next round
        Any -> {Any, Key}
      end
    end,

    {ok, undefined}, % initial accumulator
    KeysSorted       % data
  ),

  CheckResult.

-else.

% check if given list of #interval structures is valid (From =< To) and
% intervals are not overlapping
check_new_values (Intervals) ->
  % take keys to a tuple and sort them
  Keys = lists:sort([{F, T} || #interval{from_number = F, to_number = T} <- Intervals]),

  % check if interval borders are valid
  check_new_values_loop (Keys, undefined).

% first iteration (no PrevTo border)
check_new_values_loop ([{From, To} = Current | Tail ], undefined) when (From =< To) ->
  check_new_values_loop (Tail, Current);
% borders are valid and higher then previous border
check_new_values_loop ([{From, To} = Current | Tail ], {_, PrevTo}) when (PrevTo < From) and (From =< To) ->
  check_new_values_loop (Tail, Current);
% all intervals are checked - result is ok
check_new_values_loop([], _) -> ok;

% error - previous interval is higher
check_new_values_loop ([{From, _To} = I | _Tail ], {_, PrevTo} = Prev) when (PrevTo >= From) ->
  {error, {interval_overlap, Prev, I}};
% error - curret interval is not valid (From > To)
check_new_values_loop ([{From, To} = I | _Tail ], _Prev) when (From > To) ->
  {error, {bad_interval, I}}.

-endif.


%% ----------------------------------------------------------------------------
%%  tests
%% ----------------------------------------------------------------------------
-ifdef(EUNIT).

-define (CHECK_VAL, check_new_values).
%-define (CHECK_VAL, check_new_values).

  validation_test_ () ->
    [{"empty test",
       ?_assertEqual (ok, check_new_values(mk_data([])))},
     {"simple test",
       ?_assertEqual (ok, check_new_values(mk_data([{100, 200}])))},
     {"validation",
       ?_assertEqual ({error,{bad_interval,{310,300}}}, check_new_values(mk_data([{100, 200}, {310, 300}])))},     
     {"longer test",
       ?_assertEqual (ok, check_new_values(mk_data([{100000 + (100 *I) , 100000 + (100 *I) + 99} || I <- lists:seq(1, 100000)])))},
     {"overlap 1",
       ?_assertEqual ({error,{interval_overlap,{100,200},{200,300}}},
                      check_new_values(mk_data([{100, 200}, {200, 300}])))},
     {"overlap 2",
       ?_assertEqual ({error, {interval_overlap,{200,300},{300,400}}},
                      check_new_values(mk_data([{200,300},{100,199},{300,400}])))},
     {"single points",
       ?_assertEqual (ok,
                      check_new_values(mk_data([{100,100},{200,200},{300,300}])))},
     {"single points - overlap",
       ?_assertEqual ({error, {interval_overlap,{100,100},{100,100}}},
                      check_new_values(mk_data([{100,100},{200,200},{300,300},{100,100}])))},
    {"bad interval",
       ?_assertEqual ({error, {bad_interval,{220,215}}},
                      check_new_values(mk_data([{100,110},{220,215}])))},
    {"bad interval first",
       ?_assertEqual ({error, {bad_interval,{220,215}}},
                      check_new_values(mk_data([{220,215}, {300, 310}])))}
    ].

  % test helper
  mk_data (List) ->
    [#interval{from_number = F, to_number = T} || {F, T} <- List ].
-endif.
