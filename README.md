## Charon-FS Overview
Charon is a POSIX-like data-centric cloud-backed file system designed to store and share big data in a secure, reliable and efficient way using multiple cloud providers.


## Getting Started

### Pre-requisites

##### Environment
Charon is implemented in Java as a FUSE file system (file system in userspace). Before installing and running Charon make sure your environment matches the following requisites:
- [x] Linux platform
- [x] java 7+
- [x] fuse library (sudo apt-get install libfuse-dev)

##### Cloud Storage Providers
Although Charon can work virtually with any cloud storage provider available, this prototype supports [Amazon S3](https://aws.amazon.com/s3/), [Google Storage](https://cloud.google.com/storage/), [RackSpace Files](https://www.rackspace.com/cloud/files), [Windows Azure Storage](https://azure.microsoft.com/services/storage/blobs/). You must have/create an account on each one of these providers.

To help you finding the API credentials to use in Charon please follow the tips below:

* To find the Amazon S3 keys, go to the AWS Management Console and click in S3 service. In the upper right corner, click in your account name and enter in the Security Credentials. After that, in the Access Keys separator you may generate your access and secret keys.

* To find the Google Storage keys, go to the Google API Console, pick the project you have created before. In the upper left corner, open the galery list and go to the Storage separator. Choose the Configuration tab on the left side, and go to the Interoperable Access. There you shall find your keys.

* To find the RackSpace keys, go to the Control Panel. In the upper right corner, go to the My Profile & Settings tab and look for your Rackspace API Key. The access key is just your login username.

* To find the Windows Azure keys, go to the Windows Azure portal. Create a new storage project. Select this new project and then go to the Access Keys tab. In this case, your access key is your storage project name and you secret key is the primary key in the key management.


### Setup

##### Install
To install Charon just run the script below.

`$ ./install.sh`

##### Configuration

To run Charon you must fill two configuration files: `depsky.config` and `charon.config`

* `depsky.config`: fill the `accessKey` and `secretKey` attributes with the cloud credentials of each one of the cloud storage providers.

* `charon.config`: this file contains parameters about the system itself. Most of the attributes have default values, however you can change them as you prefer (e.g. cache location, number of parallel threads to upload/download data). The values that you must fill are the ones below:
  * `CLIENT_ID` - the user id
  * `client.name` - the user name
  * `email` - the user email
  * `mount.point` - the where the file system will be mounted

### Run

After all the configuration ended, you just need to mount the file system. To do that please run the script below:

`$ ./Charon_mount.sh`

To unmount the system you can use Ctrl^C or run the script `Charon_umount.sh`.
If you want to delete all the data that Charon have stored in your cloud provider accounts you just need to run the script `Charon_clean.sh`
