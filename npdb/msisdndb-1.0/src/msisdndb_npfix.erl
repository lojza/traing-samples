%%% ===========================================================================
%%%   Msisdn ported fixed numbers CZ database
%%% ===========================================================================
-module (msisdndb_npfix).

-export([start_link/0,start_link/1,stop/0]).
-export([insert/3, insert_async/3, insert_async/4, insert_many/1, lookup/1, read_file/1, delete_all/0, delete/1]).

-define (NPFIX_TAB_NAME, ?MODULE).
-define (NPFIX_IMPORT_MODULE, msisdndb_npfix_imp).

-include("msisdndb_logging.hrl").

%%% ---------------------------------------------------------------------------
%%%  Operation & Mainenance
%%% ---------------------------------------------------------------------------
start_link() ->
    DetsFile = msisdndb_cfg_util:db_path("msisdndb_npfix.dets"),
    start_link(DetsFile).

start_link(DetsFile) ->
    msisdndb_gendb_interval:start_link (?MODULE, ?NPFIX_TAB_NAME, ?NPFIX_IMPORT_MODULE, DetsFile).

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
    msisdndb_gendb_interval:lookup (?NPFIX_TAB_NAME, Msisdn).

read_file(FileName) ->
    msisdndb_gen_sync:replace (npfix, FileName).

delete_all () ->
    msisdndb_gendb_interval:delete_all (?MODULE).

delete (FromKey) ->
    msisdndb_gendb_interval:delete_by_key (?MODULE, FromKey).
