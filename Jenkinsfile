pipeline {
    agent any

    environment {
        IMAGE_NAME = "movie-platform"
        TAG = "latest"
    }

    stages {

        stage('Checkout') {
            steps {
                echo 'Cloning repository...'
                git branch: 'main', url: 'https://github.com/Refaat2020/Movie-platform.git'
            }
        }

        stage('Build Docker Image') {
            steps {
                echo 'Building Docker image...'
                sh 'docker build -t $IMAGE_NAME:$TAG .'
            }
        }

        stage('List Images') {
            steps {
                sh 'docker images'
            }
        }

    }

    post {
        success {
            echo 'Build completed successfully 🚀'
        }
        failure {
            echo 'Build failed ❌'
        }
    }
}