/*
 * Written in Node so we can get the job done quickly without having to install
 * the dotnet CLR.
 * 
 * Discovers which projects have changed as part of the last commit and then
 * uses the tests.json file to determine which test projects should be executed.
 * This is done to try and keep the Action as streamlined as possible given it can
 * take several minutes to start a Docker image for SQL Server, for example. We
 * should avoid doing this if there are no SQL Server changes.
 */

'use strict';

const fs = require('fs');
const exec = require('child_process').exec;

const rawdata = fs.readFileSync('.github/workflows/tests.json');
const tests = JSON.parse(rawdata);

exec('git diff --name-only HEAD^',
   function (error, stdout, stderr) {
    if (error) throw stderr;

    console.log('Changed files:');
    console.log();
    console.log(stdout);

    const regex = /^src\/Datalite[a-z0-9\.]*\//gmi;
    let m;
    const filters = [];
    
    while ((m = regex.exec(stdout)) !== null) {
        let path = m[0];
        let project = tests.find(x => x.path == path);

        if (!project) continue;

        for(let filter of project.filters) {
            if (filters.indexOf(filter) == -1)
                filters.push(filter);
        }
    }

    filters.push('FullyQualifiedName=ZZZ');

    console.log(filters.join('|'));
    console.log(`::set-output name=filter::${filters.join('|')}`);
   });