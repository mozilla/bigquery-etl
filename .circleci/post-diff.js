#!/usr/bin/env node
// A script for posting the generated-sql diff to Github from CircleCI.
// This requires GH_AUTH_TOKEN to be set up, along-side CircleCI specific
// variables. See the source at [1] for more details.
// https://github.com/themadcreator/circle-github-bot/blob/master/src/index.ts

const fs = require("fs");
const bot = require("circle-github-bot").create();
const { graphql } = require("@octokit/graphql");
const path = require("path");

const diff_file = "sql.diff";
// Github comments can have a maximum length of 65536 characters
const max_content_length = 65000;

async function minimize_pr_diff_comments(pr_url) {
    const graphql_authorized = graphql.defaults({
        headers: {
            authorization: `token ${process.env.GH_AUTH_TOKEN}`,
        },
    });
    const { viewer } = await graphql_authorized(
        `query {
            viewer {
                login
            }
        }`
    );
    const { repository } = await graphql_authorized(
        `query($repo_owner:String!, $repo_name:String!, $pr_number:Int!) {
            repository(owner: $repo_owner, name: $repo_name) {
                pullRequest(number: $pr_number) {
                    comments(last: 100) {
                        nodes {
                            id
                            author {
                                login
                            }
                            bodyText
                            isMinimized
                        }
                    }
                }
            }
        }`,
        {
            repo_owner: process.env.CIRCLE_PROJECT_USERNAME,
            repo_name: process.env.CIRCLE_PROJECT_REPONAME,
            pr_number: parseInt(path.basename(pr_url)),
        }
    );
    for (const comment of repository.pullRequest.comments.nodes) {
        if (
            comment.author.login === viewer.login
            && comment.bodyText.includes(diff_file)
            && !comment.isMinimized
        ) {
            await graphql_authorized(
                `mutation($comment_id:ID!) {
                    minimizeComment(input: {subjectId: $comment_id, classifier: OUTDATED}) {
                        clientMutationId
                    }
                }`,
                {
                    comment_id: comment.id,
                }
            );
        }
    }
}

function diff() {
    let root = "/tmp/workspace/generated-sql/";
    let diff_content = fs.readFileSync(root + "/" + diff_file, "utf8");

    var body = "No content detected.";
    var warnings = "";

    if (diff_content) {
        if (diff_content.length > max_content_length) {
            diff_content = diff_content.substring(0, max_content_length);
            warnings = "⚠️ Only part of the diff is displayed."
        }
        body = `<details>
<summary>Click to expand!</summary>

\`\`\`diff
${diff_content}
\`\`\`

</details>
`
    }
    var content = `#### \`${diff_file}\`
${body}
${warnings}
`;
    return content;
}

if (process.env.CIRCLE_PULL_REQUEST) {
    minimize_pr_diff_comments(process.env.CIRCLE_PULL_REQUEST);
}

bot.comment(process.env.GH_AUTH_TOKEN, `
### Integration report for "${bot.env.commitMessage}"
${diff()}

[Link to full diff](https://output.circle-artifacts.com/output/job/${process.env.CIRCLE_WORKFLOW_JOB_ID}/artifacts/${process.env.CIRCLE_NODE_INDEX}/sql.diff)
`);
