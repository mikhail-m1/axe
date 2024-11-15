use anyhow::Result;
use egui::{FontFamily, Label, RichText, Widget};
use egui_extras::Column;

pub fn run(lines: Lines) -> Result<()> {
    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_min_inner_size([600.0, 400.0])
            .with_icon(
                eframe::icon_data::from_png_bytes(&include_bytes!("../assets/icon-256.png")[..])
                    .expect("Failed to load icon"),
            ),
        ..Default::default()
    };

    eframe::run_native(
        "cw-axe",
        native_options,
        Box::new(|c| Box::new(Log::new(c, lines))),
    )
    .map_err(|e| anyhow::Error::msg(e.to_string()))
}

pub type Lines = Vec<(String, String)>;

pub struct Log {
    lines: Lines,
}

impl Log {
    pub fn new(_cc: &eframe::CreationContext<'_>, lines: Lines) -> Self {
        Self { lines }
    }
}
impl eframe::App for Log {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        egui::TopBottomPanel::top("top_panel").show(ctx, |ui| {
            egui::menu::bar(ui, |ui| {
                egui::widgets::global_dark_light_mode_buttons(ui);
            });
        });
        egui::CentralPanel::default().show(ctx, |ui| {
            egui_extras::TableBuilder::new(ui)
                .auto_shrink(egui::Vec2b::new(false, false))
                .resizable(true)
                .column(Column::auto().clip(true))
                .column(Column::remainder().clip(true))
                .max_scroll_height(f32::INFINITY)
                .sense(egui::Sense::click())
                .header(20.0, |mut header| {
                    header.col(|ui| {
                        ui.strong("Timestamp");
                    });
                    header.col(|ui| {
                        ui.strong("Message");
                    });
                })
                .body(|body| {
                    body.rows(20., self.lines.len(), |mut row| {
                        let index = row.index();
                        row.col(|ui| {
                            Label::new(
                                RichText::new(&self.lines[index].0).family(FontFamily::Monospace),
                            )
                            .wrap(false)
                            .ui(ui);
                        });
                        row.col(|ui| {
                            Label::new(
                                RichText::new(&self.lines[index].1).family(FontFamily::Monospace),
                            )
                            .truncate(true)
                            .ui(ui);
                        });
                        if row.response().clicked() {
                            // TODO show window
                            // egui::Window::new("message").show(ctx, |ui| {});
                        }
                    });
                });
        });
    }
}
