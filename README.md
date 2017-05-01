# s3-time-stream

cli tool. stream an exact time range from logs stored in s3.
find the time segment with binary search so you download the minimum data possible.

```sh
s3-time-stream --bucket log-bucket --start 2017-04-27T16:08:00.212Z --end 2017-04-27T16:08:02.212Z
```


expects that logs are stored in s3 with date+hour prefix.

`s3://bucket/2017-04-19T20`

expects log lines to start with date in this format right now.

`<134>2017-04-27T16:08:00Z `

there can be many logs per hour they will be streamed in time order even if they overlap.

both of these things should be configurable later. e_notime
