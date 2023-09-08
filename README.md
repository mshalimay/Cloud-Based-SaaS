# Short description
Cloud-based SaaS platform for compute-intensive genetic annotation tasks, utilizing a three-server architecture for web rendering, computation, and user notifications. Uses REST protocols for communication and Flask for server-side logic.

Deployment of the platform is on AWS using following services: EC2, EBS, S3, DynamoDB, SQS, SNS, Glacier, Lambda functions, Step Functions, Security Groups, and ELB. Integrated Stripe and Globus for payment and user authentication.

This project was developed as part of UChicago MPCS Cloud Computing course created by Professor Vas Vasiliadis.

Directory contents are as follows:
* `/web` - The web app files
* `/ann` - Annotator files
* `/util` - Utility scripts/apps for notifications, archival, and restoration
* `/aws` - AWS user data files

TODO: complete the description of this project; include snapshots of website.
