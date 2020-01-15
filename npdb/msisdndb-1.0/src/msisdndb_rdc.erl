%%% ===========================================================================
%%%   Msisdn RDC CZ database
%%% ===========================================================================
-module (msisdndb_rdc).

-export([start_link/0, start_link/1, stop/0]).
-export([insert/3, insert_async/3, insert_async/4, insert_many/1, lookup/1, delete_all/0, read_file/1, delete/1]).

-define (RDC_TAB_NAME, ?MODULE).
-define (RDC_IMPORT_MODULE, msisdndb_rdc_imp).

-include("msisdndb_logging.hrl").

%%% ---------------------------------------------------------------------------
%%%  Operation & Mainenance
%%% ---------------------------------------------------------------------------
start_link() ->
    DetsFile = msisdndb_cfg_util:db_path("msisdndb_rdc.dets"),
    start_link(DetsFile).

start_link(DetsBackup) ->
    msisdndb_gendb_interval:start_link (?MODULE, ?RDC_TAB_NAME, ?RDC_IMPORT_MODULE, DetsBackup).

stop() ->
    msisdndb_gendb_interval:stop(?MODULE).

%%% ---------------------------------------------------------------------------
%%%  API function
%%% ---------------------------------------------------------------------------
insert (MsisdnFrom, MsisdnTo, OperId) ->
    msisdndb_gendb_interval:insert (?MODULE, MsisdnFrom, MsisdnTo, OperId).

insert_async (MsisdnFrom, MsisdnTo, OperId) ->
    msisdndb_gendb_interval:insert_async (?MODULE, MsisdnFrom, MsisdnTo, OperId).

insert_async (MsisdnFrom, MsisdnTo, OperId, From) ->
    msisdndb_gendb_interval:insert_async (?MODULE, MsisdnFrom, MsisdnTo, OperId, From).

insert_many (Values) ->
    msisdndb_gendb_interval:insert_many (?MODULE, Values).

lookup (Msisdn) ->
    msisdndb_gendb_interval:lookup (?RDC_TAB_NAME, Msisdn).

delete_all() ->
    msisdndb_gendb_interval:delete_all (?MODULE).

read_file(FileName) ->
    msisdndb_gen_sync:replace (rdc, FileName).

delete (FromKey) ->
    msisdndb_gendb_interval:delete_by_key (?MODULE, FromKey).
