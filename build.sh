#!/usr/bin/env bash

set -o errexit;

dotnet build -c Release;

project_dir="$(dirname "$(realpath "$0")")";
project_name="$(jq -r .project.restore.projectName obj/project.assets.json)";
version="$(jq -r .version assets/modinfo.json)";
# TODO: Update project version based on modinfo.json

mkdir -p "$project_dir/builds";
outfile="$project_dir/builds/$project_name-$version.zip";
tmpdir="$(mktemp -d)";

cp -v "bin/Release/Mods/$project_name.dll"  "$tmpdir/";
cp -v "bin/Release/Mods/$project_name.pdb"  "$tmpdir/";
cp -v assets/* "$tmpdir/";
pushd "$tmpdir";
zip -r "$outfile" .;
popd;

printf "\nBuild saved to %s\n" "$outfile";
