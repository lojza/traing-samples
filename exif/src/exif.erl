%%
%% Main API module
%%

-module(exif).

-export([read_file/1, read_file2/1, read_binary/1, read_dir/1]).

-export_type([exif_info/0, exif_value/0]).

-type exif_info() :: [{Tag :: integer(), Value :: exif_value() | [exif_value()]}].
-type exif_value() :: integer() |               % integer types (short, long ...) 
                      {integer(), integer()} |  % real types
                      binary().                 % string or raw type

%%
%% Read Exif infomation from file.
%%

-spec read_file(FileName) -> {ok, ExifInfo} | {error, Reason} when
      FileName :: file:name_all(),
      ExifInfo :: exif_info(),
      Reason :: not_found | term().

read_file(FileName) ->
    case file:open(FileName, [read, raw]) of
        {ok, IoDevice} ->
            Result = read_file_loop(IoDevice, undefined),
            file:close(IoDevice),
            Result;
        {error, Reason} -> {error, Reason}
    end.

read_file_loop(IoDevice, Buff) ->
    case file:read(IoDevice, 1) of
        {ok, Bin0} ->
            tu je nekde chyba
            Bin = case Buff of
                      undefined -> Bin0;
                      B -> list_to_binary([B, Bin0])
                  end,
            case read_binary(Bin) of
                {error, need_more_data} ->
                    read_file_loop(IoDevice, Bin);
                Result -> Result
            end;
        eof -> {error, eof};
        {error, Reason} -> {error, Reason}
    end.

read_file2(FileName) ->
    case file:read_file(FileName) of
        {ok, Bin} -> read_binary(Bin);
        {error, Reason} -> {error, Reason}
    end.


%%
%% Read Exif from binary data
%%

-spec read_binary(Bin) -> {ok, ExifInfo} | {error, Reason} when
      Bin :: binary(),
      ExifInfo :: exif_info(),
      Reason :: not_found | term().

read_binary(Bin) ->
    exif_codec:jpeg_exif(Bin).

read_dir(Dir) ->
    filelib:fold_files(Dir, "", true, 
                       fun (FileName, {NOk, NErr, NExit} = Acc0) ->
                           case filelib:is_regular(FileName) of
                               true ->
                                   Ext = filename:extension(FileName),
                                   LcExt = string:to_lower(Ext),
                                   case lists:member(LcExt, [".jpg", ".jpeg"]) of
                                       true ->
                                           case catch read_file(FileName) of
                                               {ok, _} -> {NOk + 1, NErr, NExit};
                                               {error, _Reason} ->
                                                   io:format("~p ~p~n", [FileName, 'error']),
                                                   {NOk, NErr + 1, NExit};
                                               {'EXIT', _Reason} ->
                                                   io:format("~p ~p~n", [FileName, 'EXIT']),
                                                   {NOk, NErr, NExit + 1}
                                           end;
                                       false -> Acc0
                                   end;
                               false -> Acc0
                           end
                       end, {0, 0, 0}).
