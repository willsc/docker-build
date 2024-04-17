# FILEPATH: /Users/cwills/docker-build/Jenkinsfile

pipeline {
  agent any
  options {
    buildDiscarder(logRotator(numToKeepStr: '5', daysToKeepStr: '5'))
    timestamps()
  }
  environment {
    registry="caronwills/docker-repo"
    registryCredential='Docker-repo'
  }
  
  stages {
    stage('Install Docker') {
      steps {
        sh 'apt-get update'
        sh 'apt-get install -y docker'
      }
    }
    stage 'Building image' {
      steps {
        script {
          dockerImage=$(docker build $registry:$BUILD_NUMBER)
        }
      }
    }
    stage 'Deploy Image' {
      steps {
        script {
          docker login -u "$DOCKER_USERNAME" -p "$DOCKER_PASSWORD"
          docker push $registry:$BUILD_NUMBER
        }
      }
    }
  }
}
