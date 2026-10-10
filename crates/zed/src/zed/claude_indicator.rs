//! A status-bar indicator for Claude Code.
//!
//! Shows a Claude icon in Zed's status bar. Clicking it opens a terminal
//! running `claude`, which auto-connects to Zed via the lockfile the Claude
//! Code IDE server writes — so you don't have to launch/connect manually each
//! session. (JetBrains-plugin parity: a clickable Claude status icon.)

use collections::HashMap;
use gpui::{App, Context, div, ParentElement, Render, WeakEntity, Window};
use task::{HideStrategy, RevealStrategy, SaveStrategy, Shell, SpawnInTerminal, TaskId};
use ui::{Clickable, IconButton, IconButtonShape, IconName, IconSize};
use workspace::{HideStatusItem, ItemHandle, StatusItemView, Workspace};
use zed_actions::RevealTarget;

pub struct ClaudeIndicator {
    workspace: WeakEntity<Workspace>,
}

impl ClaudeIndicator {
    pub fn new(workspace: WeakEntity<Workspace>) -> Self {
        Self { workspace }
    }
}

impl Render for ClaudeIndicator {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl gpui::IntoElement {
        let workspace = self.workspace.clone();
        div().child(
            IconButton::new("claude-code-indicator", IconName::AiClaude)
                .icon_size(IconSize::Small)
                .shape(IconButtonShape::Square)
                .on_click(cx.listener(move |_, _, window, cx| {
                    let spawn_in_terminal = SpawnInTerminal {
                        id: TaskId("claude".to_string()),
                        full_label: "claude".to_string(),
                        label: "Claude Code".to_string(),
                        command: Some("claude".to_string()),
                        args: Vec::new(),
                        command_label: "claude".to_string(),
                        cwd: None,
                        env: HashMap::default(),
                        use_new_terminal: true,
                        allow_concurrent_runs: true,
                        reveal: RevealStrategy::NoFocus,
                        reveal_target: RevealTarget::Dock,
                        hide: HideStrategy::Never,
                        shell: Shell::System,
                        show_summary: false,
                        show_command: false,
                        show_rerun: false,
                        save: SaveStrategy::default(),
                    };
                    workspace
                        .update(cx, |workspace, cx| {
                            workspace
                                .spawn_in_terminal(spawn_in_terminal, window, cx)
                                .detach();
                        })
                        .ok();
                })),
        )
    }
}

impl StatusItemView for ClaudeIndicator {
    fn set_active_pane_item(
        &mut self,
        _active_pane_item: Option<&dyn ItemHandle>,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) {
    }

    fn hide_setting(&self, _cx: &App) -> Option<HideStatusItem> {
        None
    }
}
