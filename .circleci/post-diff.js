#!/usr/bin/env node
// A script for posting the generated-sql diff to Github from CircleCI. 
// This requires GH_AUTH_TOKEN to be set up, along-side CircleCI specific 
// variables. See the source at [1] for more details.
// https://github.com/themadcreator/circle-github-bot/blob/master/src/index.ts

const fs = require("fs");
const bot = require("circle-github-bot").create();
// Github comments can have a maximum length of 65536 characters
const max_content_length = 65000;

function diff() {
    let root = "/tmp/workspace/generated-sql/";
    let diff_file = "sql.diff";
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

bot.comment(process.env.GH_AUTH_TOKEN, `
### Integration report for "${bot.env.commitMessage}"
${diff()}

[Link to full diff](https://output.circle-artifacts.com/output/job/${process.env.CIRCLE_WORKFLOW_JOB_ID}/artifacts/${process.env.CIRCLE_NODE_INDEX}/sql.diff)
`);
