ssh bitnami@$2 'export AWS_ACCESS_KEY='"'$AWS_ACCESS_KEY'"'; export AWS_SECRET_KEY='"'$AWS_SECRET_KEY'"'; bash -s' < $1
