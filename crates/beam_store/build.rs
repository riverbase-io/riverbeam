// `embed_migrations!` cannot see a new migration directory on its own: rebuild
// when `migrations/` changes (diesel_migrations' documented fix).
fn main() {
    println!("cargo:rerun-if-changed=migrations");
}
