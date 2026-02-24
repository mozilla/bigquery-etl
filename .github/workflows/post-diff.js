#!/usr/bin/env node
// A script for posting the generated SQL diff to Github from GitHub Actions.

const fs = require("fs");
const { graphql } = require("@octokit/graphql");

const diff_file = "sql.diff";
const graphql_authorized = graphql.defaults({
    headers: {
        authorization: `token ${process.env.GITHUB_TOKEN}`,
    },
});
// Github comments can have a maximum length of 65536 characters
const max_content_length = 65000;

async function minimize_pr_diff_comments() {
    if (!process.env.PR_NUMBER) {
        return;
    }
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
            repo_owner: process.env.REPO_OWNER,
            repo_name: process.env.REPO_NAME,
            pr_number: parseInt(process.env.PR_NUMBER),
        }
    );
    for (const comment of repository.pullRequest.comments.nodes) {
        if (
            comment.author.login === viewer.login
            && comment.bodyText.includes(diff_file)
            && !comment.isMinimized
        ) {
            console.log(`Minimizing comment ${comment.id}.`);
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
    let root = "/tmp/workspace/";
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

${warnings}

[Link to full diff](https://github.com/${process.env.REPO_OWNER}/${process.env.REPO_NAME}/actions/runs/${process.env.RUN_ID}#summary-)
`
    }
    var content = `#### \`${diff_file}\`
${body}
`;
    return content;
}

async function post_diff() {
    if (!process.env.PR_NUMBER) {
        console.log("No PR number found, skipping comment posting.");
        return;
    }

    const commit_message = process.env.COMMIT_MESSAGE || "this commit";
    const content = `### Integration report for "${commit_message}"
${diff()}
`;

    const { repository } = await graphql_authorized(
        `query($repo_owner:String!, $repo_name:String!, $pr_number:Int!) {
            repository(owner: $repo_owner, name: $repo_name) {
                pullRequest(number: $pr_number) {
                    id
                }
            }
        }`,
        {
            repo_owner: process.env.REPO_OWNER,
            repo_name: process.env.REPO_NAME,
            pr_number: parseInt(process.env.PR_NUMBER),
        }
    );

    await graphql_authorized(
        `mutation($subject_id:ID!, $body:String!) {
            addComment(input: {subjectId: $subject_id, body: $body}) {
                clientMutationId
            }
        }`,
        {
            subject_id: repository.pullRequest.id,
            body: content,
        }
    );
}

minimize_pr_diff_comments().then(post_diff);
