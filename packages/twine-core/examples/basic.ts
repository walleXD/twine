import * as twine from "../src";
import { z } from "zod";
import axios from "axios";

// Define our data types
type User = {
  id: number;
  name: string;
  username: string;
  email: string;
};

type TransformedUser = {
  id: number;
  username: string;
  email: string;
};

// Define our schemas

const userSchema = z.object({
  id: z.number(),
  name: z.string(),
  username: z.string(),
  email: z.string(),
});

const transformedUserSchema = z.object({
  id: z.number(),
  username: z.string(),
  email: z.string(),
});

// Create our graph

const graph = twine
  .createTwine()
  .effect("Fetch Users", z.any(), async (): Promise<User[]> => {
    const response = await axios.get(
      "https://jsonplaceholder.typicode.com/users"
    );
    return response.data;
  })
  .map("Transform Users", userSchema, (user): TransformedUser => {
    return {
      id: user.id,
      username: user.username,
      email: user.email,
    };
  })
  .effect("Load Users", z.array(transformedUserSchema), (users): void => {
    console.log("Loaded Users:", users);
  })
  .build();

// Run our ETL process

const run = twine.bootstrap(graph);
run({}, {}).catch((error) => console.error(error));
