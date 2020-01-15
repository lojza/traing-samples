%%% ===========================================================================
%%%   Msisdn ported mobile numbers CZ database
%%% ===========================================================================
-module (msisdndb_npmob).

-export([start_link/0,start_link/1,stop/0]).
-export([insert/2, insert_async/2, insert_async/3, insert_many/1, lookup/1, delete/1, delete_all/0, read_file/1, compact_file/0]).
-export([init/1, terminate/2, handle_call/3, handle_cast/2,handle_info/2, code_change/3]).

-behavior(gen_server).

-record (loop_data, {tab_id, dets_name}).

-define (NPMOB_TAB_NAME, ?MODULE).

-include("msisdndb_logging.hrl").

-define (LONG_TIMEOUT, 30000).

%%% ---------------------------------------------------------------------------
%%%  Operation & Mainenance
%%% ---------------------------------------------------------------------------
start_link() ->
    DetsFile = msisdndb_cfg_util:db_path("msisdndb_npmob.dets"),
    start_link(DetsFile).

start_link(DetsFile) ->
    gen_server:start_link ({local, ?MODULE}, ?MODULE, [DetsFile], []).

stop() ->
    gen_server:cast(?MODULE, stop).

%%% ---------------------------------------------------------------------------
%%%  API function
%%% ---------------------------------------------------------------------------
insert (Msisdn, OperId) ->
    gen_server:call(?MODULE, {insert, Msisdn, OperId}).

insert_many (Values) ->
    gen_server:call(?MODULE, {insert_many, Values}, ?LONG_TIMEOUT).

insert_async (Msisdn, OperId) ->
    insert_async (Msisdn, OperId, self()).

insert_async (Msisdn, OperId, From) ->
    gen_server:cast(?MODULE, {insert_async, Msisdn, OperId, From}).

delete (Msisdn) ->
    gen_server:call(?MODULE, {delete, Msisdn}).

delete_all () ->
    gen_server:call(?MODULE, delete_all).

lookup (Msisdn) ->
    case ets:lookup(?MODULE, Msisdn) of
        [{Msisdn, OperId}] -> {ok, OperId};
        [] -> {error, instance}
    end.

read_file(FileName) ->
    msisdndb_gen_sync:replace (npmob, FileName).

compact_file () ->
    gen_server:call(?MODULE, compact_file, ?LONG_TIMEOUT * 100).

%sync_file(FileName) ->

%%% ---------------------------------------------------------------------------
%%%  gen_server callbacks
%%% ---------------------------------------------------------------------------

init ([DetsFile]) ->
    erlang:process_flag(trap_exit, true),
    TabName = ?NPMOB_TAB_NAME,
    TabId = ets:new (?NPMOB_TAB_NAME, [set, named_table, {keypos, 1}, {read_concurrency, true}]),

    DTN =
      if
        (DetsFile == undefined) -> undefined;
        true ->
            DetsName = {dets_backup, TabName},
            {ok, DetsName} = dets:open_file (DetsName, [{type, set}, {file, DetsFile}, {keypos, 1}]),
            ?LOG_INFO("restoring backup from ~p", [DetsFile]),
            ets:from_dets (TabName, DetsName),
            DetsName
      end,

    LoopData = #loop_data{tab_id = TabId, dets_name = DTN},
    {ok, LoopData}.

terminate (_Reason, #loop_data{}) ->
    ets:delete (?NPMOB_TAB_NAME),
    ok.

handle_cast ({insert_async, Msisdn, OperId, _From}, #loop_data{dets_name = DetsName} = LoopData) ->
    NewItem = {Msisdn, OperId},
    ets:insert (?NPMOB_TAB_NAME, NewItem),
    if
        DetsName /= undefined ->
            ok = dets:insert(DetsName, NewItem);
        true -> ok
    end,
    {noreply, LoopData};

handle_cast (stop, LoopData) ->
    {stop, normal, LoopData};

handle_cast (Request, LoopData) ->
    ?LOG_ERROR("~p recieved unexpected cast: ~p", [?MODULE, Request]),
    {noreply, LoopData}.

handle_call ({insert, Msisdn, OperId}, _From, #loop_data{dets_name = DetsName} = LoopData) ->
    NewItem = {Msisdn, OperId},
    Reply = ets:insert (?NPMOB_TAB_NAME, {Msisdn, OperId}),
    if
        DetsName /= undefined ->
            ok = dets:insert(DetsName, NewItem);
        true -> ok
    end,
    {reply, Reply, LoopData};

handle_call ({insert_many, Values}, _From, #loop_data{dets_name = DetsName} = LoopData) ->
    Reply = case validate_many (Values) of
      ok ->
        R = ets:insert (?NPMOB_TAB_NAME, Values),
        if
            DetsName /= undefined ->
                ok = dets:insert(DetsName, Values);
                %dets:from_ets(DetsName, ?NPMOB_TAB_NAME);
                %ok;
            true -> ok
        end,
        R;
      Err -> Err
    end,
    {reply, Reply, LoopData};

handle_call ({delete, Msisdn}, _From, #loop_data{dets_name = DetsName} = LoopData) ->
    Reply = ets:delete (?NPMOB_TAB_NAME, Msisdn),
    if
        DetsName /= undefined ->
            ok = dets:delete(DetsName, Msisdn);
        true -> ok
    end,
    {reply, Reply, LoopData};

handle_call (delete_all, _From, #loop_data{dets_name = DetsName} = LoopData) ->
    ?LOG_INFO("NPMOB delete_all", []),
    Reply = ets:delete_all_objects (?NPMOB_TAB_NAME),
    if
        DetsName /= undefined ->
            ok = dets:delete_all_objects(DetsName);
        true -> ok
    end,
    {reply, Reply, LoopData};

handle_call (compact_file, _From, #loop_data{dets_name = DetsName} = LoopData) ->
    Reply = if
        DetsName /= undefined -> compact_disk_file (DetsName);
        true -> no_dets
    end,
    {reply, Reply, LoopData};

handle_call (Request, _From, LoopData) ->
    ?LOG_ERROR("~p recieved unexpected call: ~p", [?MODULE, Request]),
    {noreply, LoopData}.

handle_info (Msg, LoopData) ->
    ?LOG_ERROR("~p recieved unexpected messange: ~p", [?MODULE, Msg]),
    {noreply, LoopData}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

validate_many ([Item | Tail]) ->
  case Item of
    {_Msisdn, _OperId} -> validate_many (Tail);
    I -> {error, {unexpected_item, I}}
  end;
validate_many ([]) -> ok.
%validate_many (_) -> ok.

compact_disk_file (undefined) -> ok;
compact_disk_file (DetsName) ->
  case dets:info(DetsName) of
    undefined -> {error, not_open};
    Info ->
      OpenArgs = [ begin
                     Value = proplists:get_value (Key, Info),
                     OutKey = case Key of
                               filename -> file;
                               Any -> Any
                              end,
                     {OutKey, Value}
                   end || Key <- [ type, keypos, filename ]],
      case dets:close (DetsName) of
        ok ->
          dets:open_file (DetsName, OpenArgs ++ [{repair, force}]);
        {error, Reason} -> {error, Reason}
      end
  end.
