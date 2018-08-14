twitch_stitch_root="/Users/atom-sachin/codey/twitch_stitch"

function bootstrap() {
    echo "\nbootstrappin $2"

    cd $1
    rm -rf ./venv
    deactivate
    virtualenv ./venv -p /usr/local/bin/python3
    source venv/bin/activate
    pip3 install --process-dependency-links -r requirements.txt

    cd $twitch_stitch_root
    pip3 install --process-dependency-links -e ./ts_shared/ts_aws
    pip3 install --process-dependency-links -e ./ts_shared/ts_config
    pip3 install --process-dependency-links -e ./ts_shared/ts_file
    pip3 install --process-dependency-links -e ./ts_shared/ts_http
    pip3 install --process-dependency-links -e ./ts_shared/ts_logger
    pip3 install --process-dependency-links -e ./ts_shared/ts_media

    echo "done bootstrappin $2\n"
}

find . -name "venv" -exec rm -rf '{}' +
find . -name "__pycache__" -exec rm -rf '{}' +
find $(pwd) -type f -not -path "*.serverless*" -iname "requirements.txt" -print0 | while IFS= read -r -d $'\0' file; do
    dir=$(dirname $file)
    module_name=$(basename $dir)
    if [ -z $1 ] || [ $module_name == $1 ]; then
        bootstrap $dir $module_name
    fi
done
