#!/bin/sh

# This script will delete all the objects and all the buckets from the clouds which was created by this Charon instance/user

java -cp bin:lib/*:lib/javaMail/mail.jar:lib/commons/*:lib/hdfs/*:lib/http/*:lib/depsky/*:lib/jackson/* charon.util.CleanClouds
