import * as twine from "../src";
import { z } from "zod";
import axios from "axios";

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

// Define our types
type User = z.infer<typeof userSchema>;
type TransformedUser = z.infer<typeof transformedUserSchema>;

// Create our graph
const graph = twine
  .createGraph()
  .effect("Fetch Users", z.string(), async (link): Promise<User[]> => {
    const response = await axios.get(link);
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
run("https://jsonplaceholder.typicode.com/users", {}).catch((error) =>
  console.error(error)
);
