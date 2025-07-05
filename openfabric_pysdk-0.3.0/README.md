==Openfabric-pysdk


Trigger redeploy on node2:

curl --location 'https://github.node2.openfabric.network/webhook' \
--header 'X-Hub-Signature: who cares' \
--header 'Content-Type: application/json' \
--data '{
    "ref": "master"
}'


===Canceling a request
When the user deletes a request that is not running the sdk handles it's removal

When the user requests to delete the active request, a cancel notification may be sent to the app, to handle it's imediate exit.
If the application does not implement the cancel feature or does not handle it(returns False), then the sdk will initiate the process of a shameful exit.
If the application handles the cancel feature(returns True on cancel), then the sdk will give it a predefined time to finish before initiating the shameful exit(currently hardcoded at 1.5 seconds. will likely make it a property in the future)
