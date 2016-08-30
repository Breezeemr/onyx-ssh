## onyx-ssh

Onyx plugin for ssh.

#### Installation

In your project file:

```clojure
[onyx-ssh "0.8.2.0"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.ssh])
```

#### Functions

##### sample-entry

Catalog entry:

```clojure
{:onyx/name :entry-name
 :onyx/plugin :onyx.plugin.ssh/input
 :onyx/type :input
 :onyx/medium :ssh
 :onyx/batch-size batch-size
 :onyx/doc "Reads segments from ssh"}
```

Lifecycle entry:

```clojure
[{:lifecycle/task :your-task-name
  :lifecycle/calls :onyx.plugin.ssh/lifecycle-calls}]
```

#### Attributes

|key                           | type      | description
|------------------------------|-----------|------------
|`:ssh/attr`            | `string`  | Description here.

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2015 FIX ME

Distributed under the Eclipse Public License, the same as Clojure.
