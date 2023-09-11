use maud::{
    html,
    PreEscaped,
    DOCTYPE,
};

pub fn make_html(title: &str, body: PreEscaped<String>) -> PreEscaped<String> {
    html! {
        (DOCTYPE)
        html {
            head {
                link rel="icon" type="image/png" href="!/static/favicon.png" ;
                link rel="stylesheet" href="!/static/style.css" ;

                script src="https://cdn.jsdelivr.net/npm/cbor-js@0.1.0/cbor.min.js" {};
                script src="https://cdnjs.cloudflare.com/ajax/libs/js-yaml/4.1.0/js-yaml.min.js" {};
                title {(title)}
            }
            body {
                h1 { code {(title)} }


                 (body)


            } // body
        } // html
    }
}
