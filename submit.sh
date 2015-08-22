#! /bin/zsh
echo "Comment is $1"
#source ~/.zshrc
git commit -m "$1" -a
git push origin master
