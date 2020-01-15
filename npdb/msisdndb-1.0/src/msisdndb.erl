%%% ===========================================================================
%%%   Msisdn database - main interface
%%%
%%% ===========================================================================
-module (msisdndb).

-export([start/0, start/1, start/2, stop/0]).
-export([lookup/1, lookup/2, oper_id/1, oper_id/2, info/0, info/1, print_info/0, uptime/0]).
-export([load_info/0]).

-define(APPLICATION, ?MODULE).

%%% ---------------------------------------------------------------------------
%%%   O & M functions
%%% ---------------------------------------------------------------------------

start(DetsPath, InputBasePath) ->
    start(DetsPath),
    application:set_env(?APPLICATION, input_base_path, InputBasePath),
    msisdndb_sup:start_loader().

start(DetsPath) ->
    application:set_env(?APPLICATION, db_path, DetsPath),
    start().

%% @doc This function starts the MSISDN db application.

%% @spec start() -> {ok | {error, term()}}
start() -> 
    application:start(?APPLICATION).

stop() ->
    application:stop(?APPLICATION).

%%% ---------------------------------------------------------------------------
%%%   API functions
%%% ---------------------------------------------------------------------------

oper_id(Msisdn) ->
    oper_id(Msisdn, undefined).

oper_id(Msisdn, Layers) ->
    OpLst = lookup(Msisdn),
    process_result (OpLst, Layers).


lookup (Msisdn) -> lookup (Msisdn, []).

lookup (Msisdn, _Opts) when is_integer(Msisdn) ->
    catch msisdndb_stats:increment(query_count),

    RdcResult = check_db (rdc, Msisdn),

    NpResult = case number_type (Msisdn) of
      mob -> check_db (npmob, Msisdn);
      F when (F == fix) or (F == spec) -> check_db (npfix, Msisdn);
      other -> {error, instance}
    end,

    ExtraResult = check_db (extra, Msisdn),

    [{extra, ExtraResult}, {np, NpResult}, {rdc, RdcResult}].

load_info() ->
    msisdndb_tab_loader_sup:load_info().

uptime() ->
  {UpTime, _} = erlang:statistics(wall_clock),
  {D, {H, M, S}} = calendar:seconds_to_daystime(UpTime div 1000),
  lists:flatten(io_lib:format("~p days, ~p hours, ~p minutes and ~p seconds", [D,H,M,S])).

%%% ---------------------------------------------------------------------------
%%%   local functions
%%% ---------------------------------------------------------------------------

%%
%% safe lookup into ETS database
%%
check_db (rdc, Msisdn) -> check_db_ets (msisdndb_rdc, Msisdn);
check_db (extra, Msisdn) -> check_db_ets (msisdndb_extra, Msisdn);
check_db (npfix, Msisdn) -> check_db_ets (msisdndb_npfix, Msisdn);
check_db (npmob, Msisdn) -> check_db_ets (msisdndb_npmob, Msisdn).

check_db_ets (DbModule, Msisdn) ->
    case catch DbModule:lookup(Msisdn) of
      {ok, OperId} -> {ok, OperId};
      {error, Reason} -> {error, Reason};
      Fail -> {error, {fail, Fail}}
    end.

%%
%% numner type in CZ numbering plan 
%%
number_type (Msisdn) when is_integer (Msisdn) ->
    case Msisdn of
      M when (M >= 200000000) and (M =< 599999999) -> fix;
      M when (M >= 600000000) and (M =< 799999999) -> mob;
      M when (M >= 800000000) and (M =< 999999999) -> spec;
      _ -> other
    end.

info() ->
    info ([msisdndb_extra, msisdndb_npmob, msisdndb_npfix, msisdndb_rdc]).

info (Tables) when is_list(Tables) ->
    [ info(Table) || Table <- Tables];

info(Table) when is_atom (Table) ->
    case (ets:info(Table)) of
      L when is_list(L) ->
        Size   = proplists:get_value(size, L),
        Memory = proplists:get_value(memory, L),
	{Table,[Size, Memory]};
      undefined -> {Table, table_not_found}
    end.

print_info () ->
    Info = info(),
    io:format ("+--------------------+--------------+--------------+~n"),
    io:format ("| table              |      records |  memmory [B] |~n"),
    io:format ("+--------------------+--------------+--------------+~n"),
    lists:map (
      fun ({Table, Data}) ->
          case Data of
            [Size, Memory]  -> io:format("| ~-18s | ~12B | ~12B |~n" , [Table, Size, Memory]);
            table_not_found -> io:format("| ~-18s | ~27s |~n" , [Table, "table not exists"]) 
          end
      end, Info),
    io:format ("+--------------------+--------------+--------------+~n"),
    ok.

%%
%% match result from lookup/1 with Layers (list of allowed layers or
%% undefined). Result is the first positive (ok) result which id from
%% Layers
%%
process_result ([{_LayerId, {error, Reason}} | Tail], Layers) when (Reason == instance) ->
    process_result (Tail, Layers);

process_result ([{LayerId, {ok, OperId}} | Tail], Layers) ->
    case match_layer (LayerId, Layers) of
      true  -> {ok, {OperId, LayerId}};
      false -> process_result (Tail, Layers)
    end;
process_result ([], _Layers) -> {error, instance}.

match_layer (_LayerId, undefined) -> true;
match_layer (LayerId, [LayerId | _Tail]) -> true;
match_layer (LayerId, [L | Tail]) when LayerId /= L -> match_layer (LayerId, Tail);
match_layer (_LayerId, []) -> false.
