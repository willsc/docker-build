#!/usr/bin/env groovy
pipeline {
    agent any
    stages {
        stage('Git Checkout') {
            steps {
                script {
                    git branch: 'main',
                        credentialsId: 'Jenkins-Github',
                        url: 'git@github.com:willsc/docker-build.git'
                }
            }
        }
    }
}
