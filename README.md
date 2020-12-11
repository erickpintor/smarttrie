# SmartTrie

This project has been developed during academic research. The lab book
containing details about the experiments ran has been
[published](experiments/analysis/Benchmarks.pdf) in Portuguese (BR). Please
reach out to the project's maintainers for more information.

## How to run

Requirements:

* Scala Build Tool (Sbt) 1.3.+

Commands:

* Compile: `sbt compile`
* Test: `sbt test`
* Run micro-benchmarks: `sbt jmh:run`

## Deploy

Requirements:

* Ansible 2.10.+

__Note__: Please check the `experiments/hosts` file and make sure the client and
server hosts are accessible via SSH. Run the following commands while in the
`experiments` folder.

Commands:

* Run experiment: `ansible -i hosts experiments.yaml -e state=<state>`

Available state options:

* `tree-map`: a.k.a. blocking mode;
* `trie-map`: a.k.a. non-blocking mode.

Results are stored in `experiments/results/` folder.

## License

[MIT](LICENSE.md)
