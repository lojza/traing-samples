%%% ===========================================================================
%%%   generic database server for MSISDN intervals
%%% ===========================================================================
-module (msisdndb_gendb_interval).

-export([start_link/3, start_link/4,stop/1]).
-export([insert/4, insert_async/4, insert_async/5, insert_many/2, lookup/2, delete_all/1, read_file/3, delete/3, delete_by_key/2]).
-export([init/1, terminate/2, handle_call/3, handle_cast/2,handle_info/2, code_change/3]).

-behavior(gen_server).

-record (loop_data, {tab_id, tab_name, import_module, dets_name}).

-include("msisdndb_logging.hrl").

%%% ---------------------------------------------------------------------------
%%%  Operation & Mainenance
%%% ---------------------------------------------------------------------------
start_link(ProcName, TabBaseName, ImportModule) ->
    start_link(ProcName, TabBaseName, ImportModule, undefined).

start_link(ProcName, TabBaseName, ImportModule, DetsFile) ->
    gen_server:start_link ({local, ProcName}, ?MODULE, [TabBaseName, ImportModule, DetsFile], []).

stop(ProcRef) ->
    gen_server:cast(ProcRef, stop).

%%% ---------------------------------------------------------------------------
%%%  API function
%%% ---------------------------------------------------------------------------
insert (ProcRef, MsisdnFrom, MsisdnTo, OperId) ->
    gen_server:call(ProcRef, {insert, MsisdnFrom, MsisdnTo, OperId}).

insert_async (ProcRef, MsisdnFrom, MsisdnTo, OperId) ->
    insert_async (ProcRef, MsisdnFrom, MsisdnTo, OperId, self()).

insert_async (ProcRef, MsisdnFrom, MsisdnTo, OperId, From) ->
    gen_server:cast(ProcRef, {insert_async, MsisdnFrom, MsisdnTo, OperId, From}).

insert_many (ProcRef, IntervalValues) ->
    gen_server:call(ProcRef, {insert_many, IntervalValues}).

delete (ProcRef, MsisdnFrom, MsisdnTo) ->
    gen_server:call(ProcRef, {delete, MsisdnFrom, MsisdnTo}).

delete_by_key (ProcRef, MsisdnFrom) ->
    gen_server:call(ProcRef, {delete_by_key, MsisdnFrom}).

delete_all (ProcRef) ->
    gen_server:call(ProcRef, {delete_all}).

lookup (TabId, Msisdn) ->
    case ets_interval_db:lookup(TabId, Msisdn) of
        [{_From, _To, OperId}] -> {ok, OperId};
        [] -> {error, instance}
    end.

read_file(ProcRef, ImportModule, FileName) ->
    delete_all(ProcRef),
    catch ImportModule:read_file (FileName).


%%% ---------------------------------------------------------------------------
%%%  gen_server callbacks
%%% ---------------------------------------------------------------------------

init ([TabBaseName, ImportModule, DetsFile]) ->
    erlang:process_flag(trap_exit, true),

    TabName = TabBaseName,
    TabId = ets_interval_db:new (TabName),

    DTN =
      if
        (DetsFile == undefined) -> undefined;
        true ->
            DetsName = {dets_backup, TabName},
            {ok, DetsName} = dets:open_file (DetsName, [{type, set}, {file, DetsFile}, {keypos, ets_interval_db:keypos()}]),
            ?LOG_INFO("restoring backup from ~p", [DetsFile]),
            ets:from_dets (TabName, DetsName),
            DetsName
      end,

    LoopData = #loop_data{tab_id = TabId, tab_name = TabName, dets_name = DTN, import_module = ImportModule},
    {ok, LoopData}.

terminate (_Reason, #loop_data{tab_name = TabName, dets_name = DetsName}) ->
    ets_interval_db:delete (TabName),
    if
      DetsName /= undefined -> dets:close(DetsName);
      true -> ok
    end,
    ok.

handle_cast ({insert_async, MsisdnFrom, MsisdnTo, OperId, From}, #loop_data{tab_name = TabName, dets_name = DetsName} = LoopData) ->
    case ets_interval_db:insert (TabName, MsisdnFrom, MsisdnTo, OperId) of
      {ok, NewItem} -> 
        if
          DetsName /= undefined ->
            ok = dets:insert(DetsName, NewItem),
            ok;
          true -> ok
        end,
        {noreply, LoopData};
      {error, Reason} ->
        exit(From, {insert_async_error, Reason}),
        {noreply, LoopData}
    end;

handle_cast (stop, LoopData) ->
    {stop, normal, LoopData};

handle_cast (Request, LoopData) ->
    ?LOG_ERROR("~p recieved unexpected cast: ~p", [?MODULE, Request]),
    {noreply, LoopData}.

handle_call ({insert, MsisdnFrom, MsisdnTo, OperId}, _From, #loop_data{tab_name = TabName} = LoopData) ->
    Reply = ets_interval_db:insert (TabName, MsisdnFrom, MsisdnTo, OperId),
    {reply, Reply, LoopData};

handle_call ({insert_many, IntervalValues}, _From, #loop_data{tab_name = TabName, dets_name = DetsName} = LoopData) ->
    case ets_interval_db:insert_many (TabName, IntervalValues) of
        {ok, NewItems} ->
            if
                DetsName /= undefined ->
                  ok = dets:insert(DetsName, NewItems),
                  ok;
                true -> ok
            end,
            {reply, ok, LoopData};
        {error, Reason} ->
            {reply, {error, Reason}, LoopData}
    end;

handle_call ({delete, MsisdnFrom, MsisdnTo}, _From, #loop_data{tab_name = TabName, dets_name = DetsName} = LoopData) ->
    case ets_interval_db:delete (TabName, MsisdnFrom, MsisdnTo) of
        {ok, ItemToDelete} ->
            if
                DetsName /= undefined ->
                  ok = dets:delete_object(DetsName, ItemToDelete),
                  ok;
                true -> ok
            end,
            {reply, ok, LoopData};
        {error, Reason} ->
            {reply, {error, Reason}, LoopData}
    end;

handle_call ({delete_by_key, MsisdnFrom}, _From, #loop_data{tab_name = TabName, dets_name = DetsName} = LoopData) ->
    ets_interval_db:delete_by_key (TabName, MsisdnFrom),
    if
        DetsName /= undefined ->
          ok = dets:delete(DetsName, MsisdnFrom),
          ok;
        true -> ok
    end,
    {reply, ok, LoopData};
    
handle_call ({delete_all}, _From, #loop_data{tab_name = TabName, dets_name = DetsName} = LoopData) ->
    ?LOG_INFO("~p delete_all", [TabName]),
    ets_interval_db:delete_all (TabName),
    if
        DetsName /= undefined ->
            dets:delete_all_objects (DetsName);
        true -> ok
    end,
    {reply, true, LoopData};

handle_call (Request, _From, LoopData) ->
    ?LOG_ERROR("~p recieved unexpected call: ~p", [?MODULE, Request]),
    {noreply, LoopData}.

handle_info (Msg, LoopData) ->
    ?LOG_ERROR("~p recieved unexpected messange: ~p", [?MODULE, Msg]),
    {noreply, LoopData}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
