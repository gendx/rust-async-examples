use plotters::coord::Shift;
use plotters::prelude::*;
use regex::Regex;
use std::io::BufRead;

const BAR_HEIGHT: i32 = 18;
const BAR_WIDTH: i32 = 5;

fn main() {
    let bars = parse(std::io::stdin().lock());

    let width = bars
        .iter()
        .map(|bar| bar.begin + bar.length)
        .max()
        .unwrap_or(0) as u32
        * BAR_WIDTH as u32
        + 200;
    let height = bars.len() as u32 * BAR_HEIGHT as u32 + 5;

    println!("Drawing area: {}, {}", width, height);

    let drawing_area = SVGBackend::new("plot.svg", (width, height)).into_drawing_area();
    draw(drawing_area, &bars);
}

fn draw<DB: DrawingBackend>(drawing_area: DrawingArea<DB, Shift>, bars: &[Bar]) {
    let text_style = TextStyle::from(("monospace", BAR_HEIGHT).into_font()).color(&BLACK);

    for (i, bar) in bars.iter().enumerate() {
        let i = i as i32;
        let rect = [
            (BAR_WIDTH * bar.begin, BAR_HEIGHT * i),
            (
                BAR_WIDTH * (bar.begin + bar.length) + 2,
                BAR_HEIGHT * (i + 1),
            ),
        ];
        drawing_area
            .draw(&Rectangle::new(
                rect,
                ShapeStyle {
                    color: bar.color.to_rgba(),
                    filled: true,
                    stroke_width: 0,
                },
            ))
            .unwrap();
        drawing_area
            .draw_text(
                &format!("{}({})", bar.label, bar.id),
                &text_style,
                (BAR_WIDTH * bar.begin, BAR_HEIGHT * i),
            )
            .unwrap();
    }
}

fn parse(input: impl BufRead) -> Vec<Bar> {
    let re_fetch =
        Regex::new(r"^\[(\d+)\] \#{1,2} ([a-z_]+)\((\d+)\) will complete in (\d+) ms$").unwrap();
    let re_data = Regex::new(r"^\#\# data = d:(\d+) \(([a-z]+)\)$").unwrap();

    let mut bars = Vec::new();
    for line in input.lines() {
        let line = line.unwrap();
        if let Some(caps) = re_fetch.captures(&line) {
            println!("Line matches fetching: {}", line);
            let label = caps[2].to_owned();
            let color = if label == "get_page" || label == "get_data" {
                RGBColor(0xA0, 0xC0, 0xFF)
            } else if label == "fetch_resource" {
                RGBColor(0xA0, 0xFF, 0xC0)
            } else {
                RGBColor(0xC0, 0xC0, 0xC0)
            };
            bars.push(Bar {
                begin: caps[1].parse().unwrap(),
                length: caps[4].parse().unwrap(),
                label,
                id: caps[3].parse().unwrap(),
                color,
            });
        }
        if let Some(caps) = re_data.captures(&line) {
            println!("Line matches data: {}", line);
            let id: usize = caps[1].parse().unwrap();
            let status = &caps[2];
            for bar in bars.iter_mut() {
                if bar.label == "get_data" && bar.id == id {
                    if status == "valid" {
                        bar.color = GREEN;
                    } else {
                        bar.color = RED;
                    }
                }
            }
        }
    }

    // Normalize for the starting time.
    let start = bars.iter().map(|bar| bar.begin).min().unwrap_or(0);
    for bar in bars.iter_mut() {
        bar.begin -= start;
    }

    bars
}

struct Bar {
    begin: i32,
    length: i32,
    label: String,
    id: usize,
    color: RGBColor,
}
