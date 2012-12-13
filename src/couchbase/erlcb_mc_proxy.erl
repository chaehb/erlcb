%%
%% @author CHAE, H B <chaehb@gmail.com>
%% @copyright 2012 CHAE, H B.
%%
%% @doc memcache proxy protocol processing module
%%
-module(erlcb_mc_proxy).

-author('Chae,H B<chaehb@gmail.com>').

-behaviour(gen_server).

-include("mc_constants.hrl").
-include("erlcb_memcache.hrl").
-include("erlcb.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/2]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(CouchbaseName,ModuleName) ->
    gen_server:start_link({local,ModuleName},?MODULE, [CouchbaseName,ModuleName], []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([CouchbaseName,ModuleName]) ->
	process_flag(trap_exit, true),

	NodeIndex= list_to_integer(lists:last(string:tokens(atom_to_list(ModuleName), "_"))),
	[CouchbaseData]=ets:lookup(erlcb_pool,CouchbaseName),
	[Bucket,Password]=CouchbaseData#erlcb_couchbase.bucket,
	
	NodeData=lists:nth(NodeIndex+1,CouchbaseData#erlcb_couchbase.node),
	Host = NodeData#erlcb_node.host,
	Port = NodeData#erlcb_node.direct,
	State=erlcb_mc_protocol:connect([Host,Port,Bucket,Password]),
	
    {ok, State}.

handle_call({multi_get,{Keys}},_From,State)->
	multi_op(get,{Keys},State);

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
	case is_port(State#erlcb_memcache.socket) of
		true ->
			gen_tcp:close(State#erlcb_memcache.socket),
			ok;
		false ->
			ok
	end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
encode_multi_get_packet(Keys)->
	% {Key,VBucket}
	% each key : op -> GETQ
	KeyPackets=lists:foldl(
		fun(Packet,Acc)->
			<<Acc/binary,Packet/binary>>
		end, 
		<<>>,	
		lists:map(
			fun({Key,VBucket})->
				KeyBody=#mc_body{
					key=Key
				},
				KeyHeader=#mc_header{
					magic=?REQ_MAGIC,
					opcode=?GETKQ,
					keylen=size(Key),
					reserve=VBucket,
					bodylen=size(Key)
				},
				erlcb_mc_protocol:encode_packet(KeyHeader,KeyBody)
			end,
			Keys)
	),
	% last noop : op -> NOOP, other all 0
	NoopHeader=#mc_header{
		magic=?REQ_MAGIC,
		opcode=?NOOP,
		opaque=1
	},
	
	NoopPacket=erlcb_mc_protocol:encode_packet(NoopHeader,#mc_body{}),
	<<KeyPackets/binary,NoopPacket/binary>>.

%% ------------------------------------------------------------------
%% Multi Fetch Socket Communications
%% ------------------------------------------------------------------
multi_op(get,{Keys},State)->
	Request=encode_multi_get_packet(Keys),
	NewState=send(State,Request),
	case NewState#erlcb_memcache.connected of
		true ->
			{ResultState,Result}=recv(NewState),
			case ResultState of
				ok ->
					{Count,Rows}=encode_response(Result),
					{reply,{Count,jiffy:encode(Rows)},NewState};
				error ->
					case Result of
						closed ->
							RenewState=send(State,Request),
							case RenewState#erlcb_memcache.connected of
								true ->
									{NewResultState,NewResult}=recv(RenewState),
									case NewResultState of
										ok ->
										 	{Count,Rows}=encode_response(NewResult),
											{reply,{Count,jiffy:encode(Rows)},RenewState};
										error ->
											{reply,{?CB_RECV_ERROR,NewResult},RenewState}
									end;
								false ->
									{reply,{?CB_NOT_CONNECTED,<<>>},RenewState}
							end;
						_->
							{reply,{?CB_RECV_ERROR,Result},NewState}
					end
			end;
		false ->
			{reply,{?CB_NOT_CONNECTED,<<>>},NewState}
	end.
	
send(State,Request)->
	case gen_tcp:send(State#erlcb_memcache.socket,Request) of
		ok ->
			State;
		{error,_Reason} ->
			NewState=erlcb_mc_protocol:connect([State#erlcb_memcache.host,State#erlcb_memcache.port,State#erlcb_memcache.bucket,State#erlcb_memcache.password]),
			case NewState#erlcb_memcache.connected of
				true ->
					gen_tcp:send(NewState#erlcb_memcache.socket,Request);
				false->
					ok
			end,
			NewState
	end.
	
recv(State)->
	gen_tcp:recv(State#erlcb_memcache.socket,0).

encode_response(Bin) ->
	encode_response(0,Bin,[]).
	
encode_response(Count,<<>>,Data)->
	{Count,Data};
	
encode_response(Count,Bin,Data)->
	<<HeaderBin:?HEADER_LEN/binary,RestBin/binary>> = Bin,
	Header=read_header(HeaderBin),
	case Header#mc_header.opaque of
		1 ->
			encode_response(Count,<<>>,Data);
		_->
			BodyLen=Header#mc_header.bodylen,
			<<BodyBin:BodyLen/binary,NextBin/binary>> =RestBin,
			
			Item=jiffy:decode(read_body(Header,BodyBin)),
%			Item=read_body(Header,BodyBin),
			encode_response(Count+1,NextBin,lists:append(Data,[Item]))
	end.
	
	
read_header(Bin)->
	<<?RES_MAGIC:8, Opcode:8, KeyLen:16, ExtrasLen:8, DataType:8, Status:16, BodyLen:32, Opaque:32, CAS:64>> = Bin,
	#mc_header{
		opcode=Opcode,
		keylen=KeyLen,
		extraslen=ExtrasLen,
		datatype=DataType,
		reserve=Status,
		bodylen=BodyLen,
		opaque=Opaque,
		cas=CAS
	}.
	
read_body(Header,Bin)->
	ExtrasLen=Header#mc_header.extraslen,
	KeyLen=Header#mc_header.keylen,
	<<_Extras:ExtrasLen/binary,_Key:KeyLen/binary,Data/binary>> = Bin,
	Data.
