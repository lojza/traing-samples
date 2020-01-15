%%
%% JPG exif decoder
%%

-module(exif_codec).

-export([jpeg_exif/1]).

jpeg_exif(Bin0) ->
    try
        begin
            Bin1 = read_SOI(Bin0),
            APP1 = read_APP1(Bin1),
            {ByteOrder, IFDBin} = read_tiff_header(APP1),
            decode_IFD(ByteOrder, IFDBin, APP1)
        end
    of
        Result -> {ok, Result}
    catch
        throw:{codec_error, Reason} -> {error, Reason}
    end.

read_SOI(<<16#ff, 16#d8, Rest/binary>>) ->Rest;
read_SOI(<<16#ff, Bin/binary>>) when byte_size(Bin) < 1 -> throw({codec_error, need_more_data});
read_SOI(_) -> throw({codec_error, invalid_SOI}).

read_APP1(<<16#ff, 16#e1, Len:16/unsigned, "Exif", _NULL:8, _Padding:8, Rest0/binary>>) ->
    Size = Len - 10,
    <<APP1:Size/bytes, _Rest/binary>> = Rest0,
    APP1;
read_APP1(<<16#ff, 16#e1, Bin/binary>>) when byte_size(Bin) < 8  -> throw({codec_error, need_more_data});
read_APP1(<<16#ff, _Tag:8, Len:16/unsigned, Rest0/binary>>) ->
    Size = Len - 2,
    <<_Value:Size/bytes, Rest/binary>> = Rest0,
    read_APP1(Rest);
read_APP1(<<16#ff, Bin/binary>>) when byte_size(Bin) < 3  -> throw({codec_error, need_more_data});
read_APP1(_Rest) ->
    throw({codec_error, exif_not_found}).

read_tiff_header(<<ByteOrderBin:2/bytes, ConstBin:2/bytes, IFDOffsetBin:4/bytes, _/binary>> = Bin) ->
    ByteOrder = case ByteOrderBin of
                    <<"II">> -> little;
                    <<"MM">> -> big;
                    _ -> throw({codec_error, invalid_byte_order})
                end,
    IFDOffset = to_int(ByteOrder, 32, IFDOffsetBin),
    case to_int(ByteOrder, 16, ConstBin) of
        42 -> ok;
        C -> throw({codec_error, {invalid_tiff_header_const, C}})
    end,
    IFDBin = binary:part(Bin, IFDOffset, size(Bin) - IFDOffset),
    {ByteOrder, IFDBin};
read_tiff_header(Bin) when byte_size(Bin) < 8 -> throw({codec_error, need_more_data}).

decode_IFD(ByteOrder, <<CountBin:2/bytes, Rest/binary>>, APP1) ->
    Count = to_int(ByteOrder, 16, CountBin),
    decode_IFD_loop(ByteOrder, Count, Rest, APP1);
decode_IFD(_ByteOrder, Bin, _APP1) when byte_size(Bin) < 2 -> throw({codec_error, need_more_data}).

decode_IFD_loop(ByteOrder, N, <<TagBin:2/bytes, TypeBin:2/bytes, CountBin:4/bytes, OffsetBin:4/bytes, Rest/binary>>, Block) when N > 0 ->
    Tag = to_int(ByteOrder, 16, TagBin),
    Type = to_int(ByteOrder, 16, TypeBin),
    Count = to_int(ByteOrder, 32, CountBin),
    Size = case Type of
               1 -> 1;
               2 -> 1;
               3 -> 2;
               4 -> 4;
               5 -> 4 + 4;
               7 -> 1;
               9 -> 4;
               10 -> 4 + 4
           end,
    Len = Size * Count,
    Value = if 
                Len > 4 ->
                    Offset = to_int(ByteOrder,32, OffsetBin),
                    binary:part(Block, Offset, Len);
                Len =< 4 ->
                    binary:part(OffsetBin, 0, Len)
            end,
    Value1 = case Type of
                 2 -> strip_null(Value);  % string value
                 7 -> Value;              % bytes value
                 T ->
                     case Count of
                         1 -> decode_type(T, ByteOrder, Value);
                         C ->
                             [ decode_type(T, ByteOrder, V) ||  V <- split_bin(Value, Size, C) ]
                     end
             end,
    [{Tag, Value1} | decode_IFD_loop(ByteOrder, N - 1, Rest, Block)];

decode_IFD_loop(_ByteOrder, 0, _, _) ->  [];
decode_IFD_loop(_ByteOrder, _N, Bin, _Block) when byte_size(Bin) < 12 -> throw({codec_error, need_more_data}).

decode_type(1, _ByteOrder, <<N:8>>) -> N;
decode_type(3, ByteOrder, Value) -> to_int(ByteOrder, 16, Value);
decode_type(4, ByteOrder, Value) -> to_int(ByteOrder, 32, Value);
decode_type(5, ByteOrder, <<V1:4/bytes, V2:4/bytes>>) ->
    N1 = to_int(ByteOrder, 32, V1),
    N2 = to_int(ByteOrder, 32, V2),
    {N1, N2};
decode_type(9, ByteOrder, Value) -> to_slong(ByteOrder, Value);
decode_type(10, ByteOrder, <<V1:4/bytes, V2:4/bytes>>) ->
    N1 = to_slong(ByteOrder, V1),
    N2 = to_slong(ByteOrder, V2),
    {N1, N2};
decode_type(Type, ByteOrder, Value) -> {ByteOrder, Type, Value}.

split_bin(Bin, PartSize, 1) ->
    PartSize = byte_size(Bin), % just to be sure
    [ Bin ];
split_bin(Bin, PartSize, Count) when Count > 1 ->
    <<Part:PartSize/bytes, Rest/binary>> = Bin,
    [ Part | split_bin(Rest, PartSize, Count - 1) ];
split_bin(<<>>, _PartSize, 0) -> [].

strip_null(Bin) -> strip_null(size(Bin) - 1, Bin).
strip_null(At, Bin) when At >= 0->
    case binary:at(Bin, At) of
        0 -> strip_null(At - 1, Bin);
        _ -> binary:part(Bin, 0, At + 1)
    end;
strip_null(At, _) when At < 0 -> <<>>.

to_int(big, Size, Bin) ->
    <<N:Size/big>> = Bin,
    N;
to_int(little, Size, Bin) ->
    <<N:Size/little>> = Bin,
    N.

to_slong(big, <<N:32/big-signed>>) -> N;
to_slong(little, <<N:32/little-signed>>) -> N.
