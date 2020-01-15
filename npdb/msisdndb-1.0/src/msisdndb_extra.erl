%%% ===========================================================================
%%%   Msisdn - extra numbers database
%%% ===========================================================================
-module (msisdndb_extra).

-export([start_link/0,start_link/1,stop/0]).
-export([insert/2, insert/3, insert_async/3, insert_many/1, insert_async/4, delete/2, delete_all/0, lookup/1, read_file/1, delete/1]).

-define (EXTRA_TAB_NAME, ?MODULE).
-define (EXTRA_IMPORT_MODULE, msisdndb_extra_imp).

-include("msisdndb_logging.hrl").

%%% ---------------------------------------------------------------------------
%%%  Operation & Mainenance
%%% ---------------------------------------------------------------------------
start_link() ->
    DetsFile = msisdndb_cfg_util:db_path("msisdndb_extra.dets"),
    start_link(DetsFile).

start_link(DetsFile) ->
    msisdndb_gendb_interval:start_link (?MODULE, ?EXTRA_TAB_NAME, ?EXTRA_IMPORT_MODULE, DetsFile).

stop() ->
    msisdndb_gendb_interval:stop(?MODULE).

%%% ---------------------------------------------------------------------------
%%%  API function
%%% ---------------------------------------------------------------------------
insert (Msisdn, OperId) ->
    insert (Msisdn, Msisdn, OperId).

insert (MsisdnFrom, MsisdnTo, OperId) ->
    msisdndb_gendb_interval:insert (?MODULE, MsisdnFrom, MsisdnTo, OperId).

insert_async (MsisdnFrom, MsisdnTo, OperId) ->
    msisdndb_gendb_interval:insert_async (?MODULE, MsisdnFrom, MsisdnTo, OperId).

insert_async (MsisdnFrom, MsisdnTo, OperId, From) ->
    msisdndb_gendb_interval:insert_async (?MODULE, MsisdnFrom, MsisdnTo, OperId, From).

insert_many (Values) ->
    msisdndb_gendb_interval:insert_many (?MODULE, Values).

delete (MsisdnFrom, MsisdnTo) ->
    msisdndb_gendb_interval:delete (?MODULE, MsisdnFrom, MsisdnTo).

delete_all () ->
    msisdndb_gendb_interval:delete_all (?MODULE).

lookup (Msisdn) ->
    msisdndb_gendb_interval:lookup (?EXTRA_TAB_NAME, Msisdn).

read_file(FileName) ->
    msisdndb_gen_sync:replace (extra, FileName).

delete (FromKey) ->
    msisdndb_gendb_interval:delete_by_key (?MODULE, FromKey).
