

## pre todo
install stanford-segmenter into local repository

mvn org.apache.maven.plugins:maven-install-plugin:2.3.1:install-file \
-Dfile=${projectBaseDir}/spark-core/lib/stanford-segmenter-4.2.0.jar \
-DgroupId=edu.stanford.nlp \
-DartifactId=stanford-segmenter -Dversion=4.2.0 \
-Dpackaging=jar