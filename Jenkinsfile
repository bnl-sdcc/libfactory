#!groovy

stage 'build'
node{
    checkout scm
    sh 'python setup.py bdist_rpm'
}
