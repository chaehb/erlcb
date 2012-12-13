%%
%% @author CHAE, H B <chaehb@gmail.com>
%% @copyright 2012 CHAE, H B.
%%
%% @doc Http Client Module
%%
-module(erlcb_http).
-author('Chae,H B<chaehb@gmail.com>').

-export([get/3,put/5,post/5,delete/3]).

-export([https_option/0,basic_auth_header/2]).

-include("erlcb_http.hrl").

%%==================================================================================================
%% consume restful service with httpc (in inets)
%%==================================================================================================
	
get(Url,Headers,HttpOption)->
	HttpsOption=HttpOption ++ case string:str(Url,"https") of
		1 ->
			https_option();
		_->
			[]
	end,
	
	case httpc:request(get,{Url,Headers},HttpsOption,[{body_format,binary}]) of
		{ok,{{_Version,StatusCode,_Message},_ResHeaders,ResBody}} ->
			%{ok,{StatusCode,{ResHeaders,ResBody}}};
			{StatusCode,ResBody};
		{error,{failed_coonect,_}}->
			{?FAILED_CONNECT, <<"Connection Failed">>};
		{error,{_Reason,_}}->
			%{error,Reason}
			{?INTERNAL_SERVER_ERROR,<<"Internal Server Error">>}
	end.

put(Url,Headers,HttpOption,ContentType,Body)->
	HttpsOption=HttpOption ++ case string:str(Url,"https") of
		1 ->
			https_option();
		_->
			[]
	end,
	
	case httpc:request(put,{Url,Headers, ContentType,Body},HttpsOption,[{body_format,binary}]) of
		{ok,{{_Version,StatusCode,_Message},_ResHeaders,ResBody}} ->
			%{ok,{StatusCode,{ResHeaders,ResBody}}};
			{StatusCode,ResBody};
		{error,{failed_coonect,_}}->
			{?FAILED_CONNECT, <<"Connection Failed">>};
		{error,{_Reason,_}}->
			%{error,Reason}
			{?INTERNAL_SERVER_ERROR,<<"Internal Server Error">>}
	end.

post(Url,Headers,HttpOption, ContentType,Body)->
	HttpsOption=HttpOption ++ case string:str(Url,"https") of
		1 ->
			https_option();
		_->
			[]
	end,
	
	case httpc:request(post,{Url,Headers,ContentType,Body},HttpsOption,[{body_format,binary}]) of
		{ok,{{_Version,StatusCode,_Message},_ResHeaders,ResBody}} ->
			%{ok,{StatusCode,{ResHeaders,ResBody}}};
			{StatusCode,ResBody};
		{error,{failed_coonect,_}}->
			{?FAILED_CONNECT, <<"Connection Failed">>};
		{error,{_Reason,_}}->
			%{error,Reason}
			{?INTERNAL_SERVER_ERROR,<<"Internal Server Error">>}
	end.

delete(Url,Headers,HttpOption)->
	HttpsOption=HttpOption ++ case string:str(Url,"https") of
		1 ->
			https_option();
		_->
			[]
	end,
	
	case httpc:request(delete,{Url,Headers},HttpsOption,[{body_format,binary}]) of
		{ok,{{_Version,StatusCode,_Message},_ResHeaders,ResBody}} ->
			%{ok,{StatusCode,{ResHeaders,ResBody}}};
			{StatusCode,ResBody};
		{error,{failed_coonect,_}}->
			{?FAILED_CONNECT, <<"Connection Failed">>};
		{error,{_Reason,_}}->
			%{error,Reason}
			{?INTERNAL_SERVER_ERROR,<<"Internal Server Error">>}
	end.

https_option()->
	[{ssl,[{verify,verify_none}]}].
	
basic_auth_header(User,Pass)->
	[{"Authorization","Basic "++base64:encode_to_string(lists:append([User,":",Pass]))}].
