fn main() {
    concatenate();
    aggregate();
    accumulate();
    join();
}

fn concatenate() {

    // https://docs.rs/timely/latest/timely/dataflow/operators/core/concat/trait.Concatenate.html#tymethod.concatenate

    use timely::dataflow::operators::input::Handle;
    use timely::dataflow::operators::{Concatenate, Filter, Input, Inspect};
    use timely::*;
    print!("\n\nIn function concatenate\n");

    timely::execute(Config::thread(), |worker| {
        // add an input and base computation off of it
        let mut input1 = Handle::new();
        let mut input2 = Handle::new();
        let mut input3 = Handle::new();
        worker.dataflow(|scope| {
            let stream1 = scope.input_from(&mut input1);
            let stream2 = scope.input_from(&mut input2);
            let stream3 = scope.input_from(&mut input3);

            stream1.inspect(|x| println!("stream1: {:?}", x));
            stream2.inspect(|x| println!("stream2: {:?}", x));
            stream3.inspect(|x| println!("stream3: {:?}", x));

            scope
                .concatenate(vec![stream1, stream2, stream3])
                .inspect(|x| println!("concatenated stream:{:?}", x));
        });

        let input1_data: Vec<(u32, String)> = vec![(1, String::from("M1")), (2, String::from("M4"))];
        let input2_data: Vec<(u32, String)> = vec![(1, String::from("M2")), (2, String::from("M5"))];
        let input3_data: Vec<(u32, String)> = vec![(1, String::from("M3")), (3, String::from("M6"))];


        // introduce input, advance computation
        for round in 0..2 {
            input1.send(input1_data[round].clone());
            input2.send(input2_data[round].clone());
            input3.send(input3_data[round].clone());

            input1.advance_to(round + 1);
            input2.advance_to(round + 1);
            input3.advance_to(round + 1);

            worker.step();
        }
    })
    .unwrap();
}

fn aggregate() {

    // https://docs.rs/timely/latest/timely/dataflow/operators/aggregation/aggregate/trait.Aggregate.html#required-methods

    use timely::dataflow::operators::input::Handle;
    use timely::dataflow::operators::{Concatenate, Input, Inspect};
    use timely::dataflow::operators::aggregation::Aggregate;
    use timely::*;
    print!("\n\nIn function aggregate\n");
 
    timely::execute(Config::thread(), |worker| {
        // add an input and base computation off of it
        let mut input1: Handle<usize, (u32, String)> = Handle::new();
        let mut input2 = Handle::new();
        let mut input3 = Handle::new();
        worker.dataflow(|scope| {
            let stream1 = scope.input_from(&mut input1);
            let stream2 = scope.input_from(&mut input2);
            let stream3 = scope.input_from(&mut input3);
 
            stream1.inspect(|x| println!("stream1: {:?}", x));
            stream2.inspect(|x| println!("stream2: {:?}", x));
            stream3.inspect(|x| println!("stream3: {:?}", x));
 
            let concatenated_stream = scope
                .concatenate(vec![stream1, stream2, stream3])
                .inspect(|x| println!("concatenated stream:{:?}", x));

            concatenated_stream
                .aggregate(|_key, val, agg: &mut Vec<String>| {
                        agg.push(val);
                    },
                    |key, agg: Vec<String>| (key, agg),
                    |key| *key as u64,
                )
                .inspect(|x| println!("aggregated result: {:?}", x));
        });
 
        let input1_data: Vec<(u32, String)> = vec![(1, String::from("M1")), (2, String::from("M4"))];
        let input2_data: Vec<(u32, String)> = vec![(1, String::from("M2")), (2, String::from("M5"))];
        let input3_data: Vec<(u32, String)> = vec![(1, String::from("M3")), (3, String::from("M6"))];
 
        // introduce input, advance computation
        for round in 0..2 {
            input1.send(input1_data[round].clone());
            input2.send(input2_data[round].clone());
            input3.send(input3_data[round].clone());
 
            input1.advance_to(round + 1);
            input2.advance_to(round + 1);
            input3.advance_to(round + 1);
 
            worker.step();
        }
    })
    .unwrap();
}

fn accumulate() {

    // https://docs.rs/timely/latest/timely/dataflow/operators/count/trait.Accumulate.html#required-methods

    use timely::dataflow::operators::Accumulate;
    use timely::dataflow::operators::input::Handle;
    use timely::dataflow::operators::{Concatenate, Input, Inspect};
    use timely::*;
    print!("\n\nIn function accumulate\n");
 
    timely::execute(Config::thread(), |worker| {
        // add an input and base computation off of it
        let mut input1: Handle<usize, (u32, String)> = Handle::new();
        let mut input2 = Handle::new();
        let mut input3 = Handle::new();
        worker.dataflow(|scope| {
            let stream1 = scope.input_from(&mut input1);
            let stream2 = scope.input_from(&mut input2);
            let stream3 = scope.input_from(&mut input3);
 
            stream1.inspect(|x| println!("stream1: {:?}", x));
            stream2.inspect(|x| println!("stream2: {:?}", x));
            stream3.inspect(|x| println!("stream3: {:?}", x));
 
            let concatenated_stream = scope
                .concatenate(vec![stream1, stream2, stream3])
                .inspect(|x| println!("concatenated stream:{:?}", x));

            concatenated_stream
                .accumulate(Vec::new(), |buffer, data| {
                    for data in data.iter() {
                        buffer.push(data.clone());
                    }
                })
                .inspect(|x| println!("accumulated result: {:?}", x));
        });
 
        let input1_data: Vec<(u32, String)> = vec![(1, String::from("M1")), (2, String::from("M4"))];
        let input2_data: Vec<(u32, String)> = vec![(1, String::from("M2")), (2, String::from("M5"))];
        let input3_data: Vec<(u32, String)> = vec![(1, String::from("M3")), (3, String::from("M6"))];
 
        // introduce input, advance computation
        for round in 0..2 {
            input1.send(input1_data[round].clone());
            input2.send(input2_data[round].clone());
            input3.send(input3_data[round].clone());
 
            input1.advance_to(round + 1);
            input2.advance_to(round + 1);
            input3.advance_to(round + 1);
 
            worker.step();
        }
    })
    .unwrap();
}


fn join() {

    // https://docs.rs/differential-dataflow/latest/differential_dataflow/operators/join/trait.Join.html#method.join

    use differential_dataflow::input::Input;
    use differential_dataflow::operators::Join;
    use timely;
    print!("\n\nIn function join\n");

    timely::example(|scope| {
        let (mut input1, x) = scope.new_collection();
        let (mut input2, y) = scope.new_collection();
        let (mut input3, z) = scope.new_collection();

        x.inspect(|x| println!("stream1: {:?}", x.0));
        y.inspect(|y| println!("stream2: {:?}", y.0));
        z.inspect(|z| println!("stream3: {:?}", z.0));

        let joined = x.join(&y)
                      .map(|(k, (v1, v2))| (k, (v1, v2)))
                      .join(&z)
                      .map(|(k, ((v1, v2), v3))| (k, (v1, v2, v3)));

        joined.inspect(|x| println!("joined stream: {:?}", x.0));

        let input1_data: Vec<(u32, String)> = vec![(1, String::from("M1")), (2, String::from("M4"))];
        let input2_data: Vec<(u32, String)> = vec![(1, String::from("M2")), (2, String::from("M5"))];
        let input3_data: Vec<(u32, String)> = vec![(1, String::from("M3")), (3, String::from("M6"))];

        for round in 0..2 {
            
            input1.insert(input1_data[round].clone());
            input2.insert(input2_data[round].clone());
            input3.insert(input3_data[round].clone());
        }
    });
}
