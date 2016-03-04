#!/bin/bash

export AWS_ACCESS_KEY_ID=AKIAIBJMOURP5IA2Y3OQ
export AWS_SECRET_ACCESS_KEY=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

./sbt "project mypipe" "runMain mypipe.runner.PipeRunner"
