/// Built-in skills: system prompt + tool allowlist. Hosts may add more.

#[derive(Debug, Clone)]
pub struct Skill {
    pub name: String,
    pub system_prompt: String,
    pub tools: Vec<String>,
}

#[must_use]
pub fn builtin_skills() -> Vec<Skill> {
    vec![
        Skill {
            name: "author".into(),
            system_prompt: "You are an authoring agent. Use sandbox and git tools on the change branch only. Never push protected default branches. Start with git_status."
                .into(),
            tools: vec![
                "list_tree".into(),
                "read_file".into(),
                "write_file".into(),
                "git_status".into(),
                "git_diff".into(),
                "git_log".into(),
                "git_add".into(),
                "git_commit".into(),
                "git_push".into(),
                "remember".into(),
                "search".into(),
                "get_page".into(),
                "get_block".into(),
                "graph_neighbors".into(),
                "open_change".into(),
                "propose_suggestion".into(),
                "commit_paths".into(),
                "review_change".into(),
            ],
        },
        Skill {
            name: "consume".into(),
            system_prompt: "You answer questions with citations. Do not write files or run git mutations."
                .into(),
            tools: vec![
                "search".into(),
                "get_page".into(),
                "get_block".into(),
                "graph_neighbors".into(),
            ],
        },
        Skill {
            name: "summarize".into(),
            system_prompt: "Summarize the current change or conversation. Read-only git_diff/git_log/list_tree as needed."
                .into(),
            tools: vec![
                "list_tree".into(),
                "read_file".into(),
                "git_diff".into(),
                "git_log".into(),
                "search".into(),
                "get_page".into(),
                "get_block".into(),
            ],
        },
        Skill {
            name: "commit-notes".into(),
            system_prompt: "Draft a concise commit message from git_diff and git_status. Do not commit unless asked."
                .into(),
            tools: vec!["git_status".into(), "git_diff".into(), "git_log".into()],
        },
        Skill {
            name: "review".into(),
            system_prompt: "Review the change. Cite paths. Do not merge or publish.".into(),
            tools: vec![
                "list_tree".into(),
                "read_file".into(),
                "git_diff".into(),
                "git_log".into(),
                "get_page".into(),
                "review_change".into(),
            ],
        },
        Skill {
            name: "gfs-publish".into(),
            system_prompt: "Publish a reviewed change. Always confirm before publish_docset.".into(),
            tools: vec!["publish_docset".into()],
        },
    ]
}

#[must_use]
pub fn skill_by_name(name: &str) -> Option<Skill> {
    builtin_skills().into_iter().find(|s| s.name == name)
}
