#!/usr/bin/env bash

echo "Checking if commit is release tag"

tag=`git describe --exact-match --tags HEAD`
if [[ ${tag} =~ ^([0-9]{1,2}\.[0-9]{1,2}\.[0-9]{1,2})$ ]]
then
  echo "Commit is a release, deploying artifact"
  echo "logging into azure"

  az login \
  --service-principal \
  -u ${AZURE_SP_USERNAME} \
  -p ${AZURE_SP_PASSWORD} \
  --tenant ${AZURE_SP_TENANTID}

  fn=`find ./target/ -name "*uber.jar" | rev | cut -d '/' -f1 | rev | head -n 1` # gotta love `rev`
  source=./target/${fn}
  destination=/libraries/${BUILD_DEFINITIONNAME}/${BUILD_DEFINITIONNAME}-${tag}.jar

  echo "uploading artifact from"
  echo "  | from ${source}"
  echo "  | to ${AZURE_ADLS_NAME}"
  echo "  | on path ${destination}"

  az dls fs upload \
  --account ${AZURE_ADLS_NAME} \
  --source-path ${source} \
  --destination-path ${destination}
else
  echo "Commit is not a release, not deploying artifact"
fi
