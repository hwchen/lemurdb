use csv;

error_chain! {
    foreign_links {
        Io(::std::io::Error);
        Csv(csv::Error);
        Utf8(::std::string::FromUtf8Error);
    }
}
