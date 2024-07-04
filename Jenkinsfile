#!/usr/bin/env groovy
pipeline {
    agent any
    stages {
        stage('Git Checkout') {
            steps {
                script {
                    git branch: 'main',
                        credentialsId: 'Jenkins-Github',
                        url: 'ssh://git@github.com:willsc/docker-build.git'
                }
            }
        }
    }
}
