-module(dinerl_client).
-author('Valentino Volonghi <valentino@adroll.com>').

-include("dinerl_types.hrl").

-export([api/7, api/8, api/9]).

%%
%% Item related operations
%%
-spec method_name(method()) -> string().
method_name(batch_get_item) ->
    "DynamoDB_20120810.BatchGetItem";
method_name(batch_write_item) ->
    "DynamoDB_20120810.BatchWriteItem";
method_name(get_item) ->
    "DynamoDB_20120810.GetItem";
method_name(put_item) ->
    "DynamoDBv20110924.PutItem";
method_name(delete_item) ->
    "DynamoDB_20120810.DeleteItem";
method_name(update_item) ->
    "DynamoDB_20120810.UpdateItem";

%%
%% Table related operations
%%
method_name(create_table) ->
    "DynamoDB_20120810.CreateTable";
method_name(list_tables) ->
    "DynamoDBv20110924.ListTables";
method_name(describe_table) ->
    "DynamoDBv20110924.DescribeTable";
method_name(update_table) ->
    "DynamoDBv20110924.UpdateTable";
method_name(delete_table) ->
    "DynamoDBv20110924.DeleteTable";

%%
%% query interface
%%
method_name(q) ->
    "DynamoDB_20120810.Query";
method_name(scan) ->
    "DynamoDB_20120810.Scan".


-spec api(access_key_id(), secret_access_key(), zone(),
          token(), rfcdate(), method(), any()) -> result().
api(AccessKeyId, SecretAccessKey, Zone, Token, RFCDate, Name, Body) ->
    api(AccessKeyId, SecretAccessKey, Zone, Token, RFCDate, Name, Body, undefined, []).

-spec api(access_key_id(), secret_access_key(), zone(),
          token(), rfcdate(), method(), any(), integer()) -> result().
api(AccessKeyId, SecretAccessKey, Zone, Token, RFCDate, Name, Body, Timeout) ->
    api(AccessKeyId, SecretAccessKey, Zone, Token, RFCDate, Name, Body, Timeout, []).

-spec api(access_key_id(), secret_access_key(), zone(),
          token(), rfcdate(), method(), any(), integer(), options()) -> result().
api(AccessKeyId, SecretAccessKey, Zone, Token, RFCDate, Name, Body, Timeout, Options) ->
    case dynamodb:call(AccessKeyId, SecretAccessKey, Zone, method_name(Name),
                       Token, RFCDate, jiffy:encode(Body), Timeout, Options) of
        {ok, Response} ->
            {ok, jiffy:decode(Response)};
        {error, Code, Reason} ->
            %%error_logger:info_msg("~p: ~p~n", [Name, Body]),
            {error, Code, Reason}
    end.
